import unittest

import jieba


class TestJieba(unittest.TestCase):
    def setUp(self):
        self.text = "This is a test text for jieba"

    def test_cut(self):
        self._test_cut_method(jieba.cut, ['This', ' ', 'is', ' ', 'a', ' ', 'test', ' ', 'text', ' ', 'for', ' ', 'jieba'])

    def test_cut_all(self):
        self._test_cut_method(jieba.cut, ['This', ' ', 'is', ' ', 'a', ' ', 'test', ' ', 'text', ' ', 'for', ' ', 'jieba'], cut_all=True)

    def test_cut_for_search(self):
        self._test_cut_method(jieba.cut_for_search, ['This', ' ', 'is', ' ', 'a', ' ', 'test', ' ', 'text', ' ', 'for', ' ', 'jieba'])

if __name__ == '__main__':
    unittest.main()
    def _test_cut_method(self, method, expected_result, **kwargs):
        result = method(self.text, **kwargs)
        self.assertEqual(list(result), expected_result)
