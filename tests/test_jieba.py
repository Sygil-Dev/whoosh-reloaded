import jieba
import pytest


def test_cut():
    sentence = "我来到北京清华大学"
    words = jieba.cut(sentence)
    assert list(words) == ['我', '来到', '北京', '清华大学']

def test_cut_for_search():
    sentence = "我来到北京清华大学"
    words = jieba.cut_for_search(sentence)
    assert list(words) == ['我', '来到', '北京', '清华大学']

def test_lcut():
    sentence = "我来到北京清华大学"
    words = jieba.lcut(sentence)
    assert words == ['我', '来到', '北京', '清华大学']

def test_lcut_for_search():
    sentence = "我来到北京清华大学"
    words = jieba.lcut_for_search(sentence)
    assert words == ['我', '来到', '北京', '清华大学']
