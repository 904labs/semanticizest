"""Parsing utilities for Wikipedia database dumps."""

from __future__ import print_function

from bz2 import BZ2File
from collections import Counter
import gzip
from itertools import chain
import re
import xml.etree.ElementTree as etree   # don't use LXML, it's slower (!)

import six

from semanticizest._util import ngrams


def _get_namespace(tag):
    try:
        namespace = re.match(r"^{(.*?)}", tag).group(1)
    except AttributeError:
        namespace = ''
    if not namespace.startswith("http://www.mediawiki.org/xml/export-"):
        raise ValueError("namespace %r not recognized as MediaWiki dump"
                         % namespace)
    return namespace


def extract_pages(f):
    """Extract pages from Wikimedia database dump.

    Parameters
    ----------
    f : file-like or str
        Handle on Wikimedia article dump. May be any type supported by
        etree.iterparse.

    Returns
    -------
    pages : iterable over (int, string, string)
        Generates (page_id, title, content) triples.
        In Python 2.x, may produce either str or unicode strings.
    """
    elems = (elem for _, elem in etree.iterparse(f, events=["end"]))

    # We can't rely on the namespace for database dumps, since it's changed
    # it every time a small modification to the format is made. So, determine
    # those from the first element we find, which will be part of the metadata,
    # and construct element paths.
    elem = next(elems)
    namespace = _get_namespace(elem.tag)
    ns_mapping = {"ns": namespace}
    ns_path = "./{%(ns)s}ns" % ns_mapping
    page_tag = "{%(ns)s}page" % ns_mapping
    text_path = "./{%(ns)s}revision/{%(ns)s}text" % ns_mapping
    id_path = "./{%(ns)s}id" % ns_mapping
    title_path = "./{%(ns)s}title" % ns_mapping

    for elem in elems:
        if elem.tag == page_tag:
            if elem.find(ns_path).text != '0':
                continue

            text = elem.find(text_path).text
            if text is None:
                # Empty article; these occur in Wikinews dumps.
                continue
            yield (int(elem.find(id_path).text),
                   elem.find(title_path).text,
                   text)

            # Prune the element tree, as per
            # http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
            # We do this only for <page>s, since we need to inspect the
            # ./revision/text element. That shouldn't matter since the pages
            # comprise the bulk of the file.
            elem.clear()


def extract_links(article):
    """Extract all (or most) links from article text (wiki syntax).

    Returns an iterable over (target, anchor) pairs.
    """
    links = re.findall(r"\[\[ ([^]]+) \]\] (\w*)", article, re.VERBOSE)

    for l, extra in links:
        if '|' in l:
            target, anchor = l.split('|', 1)
        else:
            target, anchor = l, l
        # If the anchor contains a colon, assume it's a file or category link.
        if ':' in target:
            continue

        anchor += extra
        yield target, anchor


def redirect(page):
    """Return redirect target for page, if any, else None."""
    m = re.match(r"\#REDIRECT \s* \[\[ ([^]]+) \]\]", page,
                 re.IGNORECASE | re.VERBOSE)
    return m and m.group(1)


_UNWANTED = re.compile(r"""
  (:?
    # we must catch nested {{}} and {| |}; allow one level of nesting
    \{ [|{] (?: \{ [|{] .*? [|}] \} | .*? )* [|}] \}
  | <math> .*? </math>
  | <ref .*? > .*? </ref>
  | \[\[ [^][:]* : (\[\[.*?\]\]|.)*? \]\]   # media, categories
  | =+ .*? =+                               # headers
  | ''+
  )
""", re.DOTALL | re.MULTILINE | re.VERBOSE)


def clean_text(page):
    """Return the clean-ish running text parts of a page."""
    return re.sub(_UNWANTED, "", page)


_LINK_SYNTAX = re.compile(r"""
    (?:
        \[\[
        (?: [^]|]* \|)?     # "target|" in [[target|anchor]]
    |
        \]\]
    )
""", re.DOTALL | re.MULTILINE | re.VERBOSE)


def remove_links(page):
    """Remove links from clean_text output."""
    page = re.sub(r'\]\]\[\[', ' ', page)       # hack hack hack, see test
    return re.sub(_LINK_SYNTAX, '', page)


def page_statistics(page, N, sentence_splitter=None, tokenizer=None):
    """Gather statistics from a single WP page.

    The sentence_splitter should be a callable that splits text into sentences.
    It defaults to an unspecified heuristic.

    See ``parse_dump`` for the parameters.

    Returns
    -------
    stats : (dict, dict)
        The first dict maps (target, anchor) pairs to counts.
        The second maps n-grams (up to N) to counts.
    """
    clean = clean_text(page)
    link_counts = Counter(extract_links(clean))

    no_links = remove_links(clean)
    if sentence_splitter is None:
        sentences = re.split(r'(?:\n{2,}|\.\s+)', no_links, re.MULTILINE)
    else:
        sentences = [sentence for paragraph in re.split('\n+', no_links)
                              for sentence in paragraph]

    if N:
        if tokenizer is None:
            tokenizer = lambda s: s.split()
        all_ngrams = chain.from_iterable(ngrams(tokenizer(sentence), N)
                                         for sentence in sentences)
        ngram_counts = Counter(all_ngrams)

    else:
        ngram_counts = None

    return link_counts, ngram_counts


def _open(f):
    if isinstance(f, six.string_types):
        if f.endswith('.gz'):
            return gzip.open(f)
        elif f.endswith('.bz2'):
            return BZ2File(f)
        return open(f)
    return f


def parse_dump(dump, db, N=7, sentence_splitter=None, tokenizer=None,
               verbose=False):
    """Parse Wikipedia database dump, return n-gram and link statistics.

    Parameters
    ----------
    dump : {file-like, str}
        Path to or handle on a Wikipedia page dump, e.g.
        'chowiki-20140919-pages-articles.xml.bz2'.
    db : SQLite connection
        Connection to database that will be used to store statistics.
    N : integer
        Maximum n-gram length. Set this to a false value to disable
        n-gram counting; this disables some of the fancier statistics,
        but baseline entity linking will still work.
    sentence_splitter : callable, optional
        Sentence splitter. Called on output of paragraph splitter
        (strings).
    tokenizer : callable, optional
        Tokenizer. Called on output of sentence splitter (strings).
        Must return iterable over strings.
    verbose : boolean
        Whether to report progress on stderr.
    """

    f = _open(dump)

    redirects = {}

    c = db.cursor()

    if verbose:
        print("Processing articles:", file=sys.stderr)
    for _, title, page in extract_pages(f):
        if verbose:
            print("    " + title, file=sys.stderr)
        target = redirect(page)
        if target is not None:
            redirects[title] = target
            continue

        link, ngram = page_statistics(page, N=N, tokenizer=tokenizer,
                                      sentence_splitter=sentence_splitter)

        tokens = chain(six.iteritems(ngram),
                       ((anchor, 0) for _, anchor in six.iterkeys(link)))
        tokens = list(tokens)
        #c.executemany('''insert or replace into ngrams (ngram, tf)
                         #values (?, ? + coalesce ((select tf from ngrams
                                                   #where ngram = ?), 0))''',
                        #((ng, count, ng)
                         #for ng, count in tokens)) # six.iteritems(ngram)))
        c.executemany('''insert or ignore into ngrams (ngram) values (?)''',
                      ((g,) for g, _ in tokens))
        c.executemany('''update ngrams set tf = tf + ?
                         where ngram = ?''',
                      tokens)

        c.executemany('''insert or ignore into linkstats values
                         ((select id from ngrams where ngram = ?), ?, 0)''',
                      ((anchor, target)
                       for target, anchor in six.iterkeys(link)))
        c.executemany('''update linkstats set count = count + ?
                         where ngram_id = (select rowid from ngrams
                                           where ngram = ?)''',
                      ((count, anchor)
                       for (_, anchor), count in six.iteritems(link)))

        db.commit()

    if verbose:
        print("Processing redirects...\n", file=sys.stderr)
    for redir, target in redirects.items():
        for anchor, count in c.execute('''select ngram_id, count from linkstats
                                          where target = ?''', [redir]):
            c.execute('''update linkstats
                         set count = count + ?
                         where target = ? and ngram_id = ?''',
                      (count, target, anchor))

        c.execute('delete from linkstats where target = ?', [redir])

    db.commit()
