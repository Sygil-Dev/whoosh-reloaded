import jieba
import pytest


def test_jieba_segmentation():
    """
    Tests the segmentation of a Chinese text string using the jieba library.
    This function does not take any parameters and does not return anything.
    """
    text = "我爱自然语言处理"
    seg_list = jieba.cut(text, cut_all=False)
    assert list(seg_list) == ['我', '爱', '自然语言处理']

def test_jieba_import():
    assert jieba is not None

def test_jieba_tokenization():
    text = "我爱自然语言处理"
    tokens = jieba.tokenize(text)
    assert list(tokens) == [('我', 0, 1), ('爱', 1, 2), ('自然语言处理', 2, 8)]
