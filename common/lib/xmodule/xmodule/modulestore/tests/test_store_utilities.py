"""
Tests for store_utilities.py
"""
import unittest
from mock import Mock
from ddt import ddt, data, unpack

from xmodule.modulestore.store_utilities import (
    get_subtree_roots, module_node_contructor
)


def mock_location(category):
    """
    Helper function to generate a mock location with a parameter-specified
    category attribute.
    """
    mock_parent_location = Mock()
    mock_parent_location.category = category
    return mock_parent_location


@ddt
class TestUtils(unittest.TestCase):
    """Tests for store_utilities"""

    ONLY_ROOTS = [
        module_node_contructor(Mock(), 'url1', 'parent1', parent_location=mock_location('vertical')),
        module_node_contructor(Mock(), 'url2', 'parent2', parent_location=mock_location('sequential')),
    ]
    ONLY_ROOTS_URLS = ['url1', 'url2']

    SUBTREES = [
        module_node_contructor(Mock(), 'url_child_1', 'url_parent_1', parent_location=mock_location('vertical')),
        module_node_contructor(Mock(), 'url_child_2', 'url_parent_1', parent_location=mock_location('vertical')),
        module_node_contructor(Mock(), 'url_parent_1', 'url_grandparent', parent_location=mock_location('sequential')),

        # NOTE: it is not actually possible for sequentials to be drafts
        # However, in this test we add a sequential to the draft tree in order
        # to check that, when using locations, get_subtree_roots automatically
        # yields any node whose parent is a sequential.
        module_node_contructor(Mock(), 'url_grandparent', 'url_great_grandparent', parent_location=mock_location('chapter')),

        module_node_contructor(Mock(), 'url_child_3', 'url_parent_2', parent_location=mock_location('vertical')),
        module_node_contructor(Mock(), 'url_child_4', 'url_parent_2', parent_location=mock_location('vertical')),
        module_node_contructor(Mock(), 'url_parent_2', 'url_grandparent_2', parent_location=mock_location('vertical')),
        module_node_contructor(Mock(), 'url_grandparent_2', 'url_great_grandparent_2', parent_location=mock_location('vertical')),
    ]

    SUBTREES_ROOTS_URLS_WITHOUT_LOCATIONS = ['url_grandparent', 'url_grandparent_2']
    # when using locations, we should also yield url_parent_1, whose parent is a sequential node
    SUBTREES_ROOTS_URLS_WITH_LOCATIONS = ['url_parent_1', 'url_grandparent', 'url_grandparent_2']

    @data(
        (ONLY_ROOTS, ONLY_ROOTS_URLS, False),
        (ONLY_ROOTS, ONLY_ROOTS_URLS, True),
        (SUBTREES, SUBTREES_ROOTS_URLS_WITHOUT_LOCATIONS, False),
        (SUBTREES, SUBTREES_ROOTS_URLS_WITH_LOCATIONS, True),
    )
    @unpack
    def test_get_subtree_roots(self, module_nodes, expected_roots_urls, use_locations):
        """tests for get_subtree_roots"""
        subtree_roots_urls = [root.url for root in get_subtree_roots(module_nodes, use_locations)]
        # make sure each root url is distinct
        self.assertEqual(len(subtree_roots_urls), len(set(subtree_roots_urls)))
        # make sure each expected url is distinct
        self.assertEqual(len(expected_roots_urls), len(set(expected_roots_urls)))
        # check that we return the expected urls
        self.assertEqual(set(subtree_roots_urls), set(expected_roots_urls))
