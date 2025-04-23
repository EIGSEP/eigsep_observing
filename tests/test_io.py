"""Tests for eigsep_corr.io"""
import copy
import os
import pytest
import numpy as np

from eigsep_corr import io


class TestFileIO:
    def test_pack_unpack_header(self):
        h1 = io.DEFAULT_HEADER
        buf = io.pack_raw_header(h1)
        h2 = io.unpack_raw_header(buf)
        assert h2["data_start"] % 8 == 0
        for k, v in h1.items():
            if type(v) is tuple or type(v) is list:
                assert tuple(v) == tuple(h2[k])
            elif type(v) is dict:
                for _k, _v in v.items():
                    assert tuple(_v) == tuple(h2[k][_k])
            elif type(v) is np.ndarray:
                np.testing.assert_allclose(v, h2[k])
            else:
                assert v == h2[k]

    def test_pack_unpack_raw_data(self):
        dt = io.build_dtype("int32", ">")
        d1 = np.ones((10, 2, 1024, 1), dtype=dt)
        buf = io.pack_raw_data(d1)
        d2 = io.unpack_raw_data(buf, '0')
        np.testing.assert_allclose(d1, d2)
        d1 = np.ones((10, 2, 1024, 2), dtype=dt)
        buf = io.pack_raw_data(d1)
        d2 = io.unpack_raw_data(buf, '02')
        np.testing.assert_allclose(d1, d2)

    def test_pack_unpack_data(self):
        h = copy.deepcopy(io.DEFAULT_HEADER)
        pairs = h["pairs"]
        d1 = {p: np.ones((10, 2, 1024, 1)) if len(p) == 1 else np.ones((10, 2, 1024, 2)) for p in pairs}
        buf = io.pack_data(d1, h)
        d2 = io.unpack_data(buf, h)
        for k, v in d1.items():
            np.testing.assert_allclose(v, d2[k])

    def test_write_read_file(self, tmp_path):
        filename = tmp_path / "test.eig"
        h1 = copy.deepcopy(io.DEFAULT_HEADER)
        pairs = h1["pairs"]
        d1 = {p: np.ones((len(h1["acc_cnt"]), 2, 1024, 1)) if len(p) == 1 else np.ones((len(h1["acc_cnt"]), 2, 1024, 2)) for p in pairs}
        io.write_file(filename, h1, d1)
        h2, d2 = io.read_file(filename)
        for k, v in d1.items():
            np.testing.assert_allclose(v, d2[k])
        for k, v in h1.items():
            if type(v) is tuple or type(v) is list:
                assert tuple(v) == tuple(h2[k])
            elif type(v) is dict:
                for _k, _v in v.items():
                    assert tuple(_v) == tuple(h2[k][_k])
            elif type(v) is np.ndarray:
                np.testing.assert_allclose(v, h2[k])
            else:
                assert v == h2[k]
