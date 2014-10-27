from cytoolz import compose

from collections import Mapping
import os.path

from nose import SkipTest
from nose.tools import (assert_equal, assert_greater, assert_in, assert_not_in,
                        assert_true)

from semanticizest._wiki_dump_parser import (clean_text, extract_links,
                                             page_statistics, parse_dump,
                                             redirect, remove_links)


def test_extract_links():
    first_link = compose(tuple, next, iter, extract_links)

    assert_equal(first_link("[[foo|bar]]"), ("foo", "bar"))
    assert_equal(first_link("[[foo]]"), ("foo", "foo"))
    assert_equal(first_link("[[File:picture!]] [[foo]]"), ("foo", "foo"))
    assert_equal(first_link("[[foo]]bar."), ("foo", "foobar"))
    assert_equal(first_link("[[baz|foobar]];"), ("baz", "foobar"))

    # This construct appears in enwiki for chemical formulae etc., but also in
    # nlwiki (and dewiki?) for more general compound nouns. The current
    # handling may not be exactly what we want; any fix should update the test
    # accordingly.
    assert_equal(list(extract_links("[[Lithium|Li]][[Fluorine|F]]")),
                 [("Lithium", "Li"), ("Fluorine", "F")])
    assert_equal(list(extract_links("[[tera-|tera]][[becquerel]]s")),
                 [("tera-", "tera"), ("becquerel", "becquerels")])


def test_page_statistics():
    page = """
        Wikisyntax is the [[syntax (to be parsed)|syntax]] used on
        [[Wikipedia]].{{citation needed|date=October 2014}}
        We have to parse it, and we use every [[hack]] in the
        [[text]][[book]] that we can find.

        And now, for something [[Wikipedia|completely]] [[hack|different]].

        Let's repeat the [[text]] to get more interesting [[statistic]]s.
        And the [[book]] too.
    """

    expected_links = {('syntax (to be parsed)', 'syntax'): 1,
                      ('Wikipedia', 'Wikipedia'): 1,
                      ('hack', 'hack'): 1,
                      ('text', 'text'): 2,
                      ('book', 'book'): 2,
                      ('Wikipedia', 'completely'): 1,
                      ('hack', 'different'): 1,
                      ('statistic', 'statistics'): 1}

    links, ngrams = page_statistics(page, N=2)

    assert_equal(dict(links), expected_links)

    assert_in('And now,', ngrams)
    assert_not_in('Wikipedia We', ngrams)
    assert_not_in('find. And', ngrams)


def test_parse_dump():
    raise SkipTest('needs to reworked with database access')
    here = os.path.dirname(os.path.abspath(__file__))
    dump = os.path.join(here, 'nlwiki-20140927-pages-articles-sample.xml')
    link_count, ngram_count = parse_dump(dump, N=2)

    assert_true(isinstance(link_count, Mapping))
    assert_true(isinstance(ngram_count, Mapping))

    # We need a better tokenizer.
    assert_in('Heinrich Tessenow,', ngram_count)
    assert_in(('Heinrich Tessenow', 'Heinrich Tessenow'), link_count)
    assert_greater(link_count[('AMX Index', 'Amsterdam Midkap Index')], 0)

    # Again, without the n-gram counting.
    link_count = parse_dump(dump, N=None)

    assert_true(isinstance(link_count, Mapping))
    assert_in('Heinrich Tessenow,', ngram_count)
    assert_in(('Heinrich Tessenow', 'Heinrich Tessenow'), link_count)


def test_redirect():
    assert_equal(redirect("#ReDiReCt [[frob]] {{R from redirect}}"), "frob")


def test_remove_links():
    text = """
        Wikisyntax is the [[syntax (to be parsed)|syntax]] used on
        [[Wikipedia]].{{citation needed|date=October 2014}}
        We have to parse it, and we use every [[hack]] in the
        [[text]][[book]] that we can find.
    """

    # Note space in "text book", inserted to match extract_links output (see
    # test above). If we don't do this, probabilities won't add up properly.
    expected = """
        Wikisyntax is the syntax used on Wikipedia.
        We have to parse it, and we use every hack in the
        text book that we can find.
    """

    assert_equal(remove_links(clean_text(text)).split(), expected.split())
