import unittest

import jieba


class TestJieba(unittest.TestCase):
    def setUp(self):
        self.text = "This is a test text for jieba"

    def test_cut(self):
        result = jieba.cut(self.text)
        self.assertEqual(list(result), ['This', ' ', 'is', ' ', 'a', ' ', 'test', ' ', 'text', ' ', 'for', ' ', 'jieba'])

    def test_cut_all(self):
        result = jieba.cut(self.text, cut_all=True)
        self.assertEqual(list(result), ['This', ' ', 'is', ' ', 'a', ' ', 'test', ' ', 'text', ' ', 'for', ' ', 'jieba'])

    def test_cut_for_search(self):
        result = jieba.cut_for_search(self.text)
        self.assertEqual(list(result), ['This', ' ', 'is', ' ', 'a', ' ', 'test', ' ', 'text', ' ', 'for', ' ', 'jieba'])

if __name__ == '__main__':
    unittest.main()
