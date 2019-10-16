#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pbq` package."""


import unittest
from unittest import mock

from pbq import PBQ
from pbq import Query


class TestPBQ(unittest.TestCase):
    """Tests for `pbq` package."""

    @mock.patch('google.cloud.bigquery.Client')
    @mock.patch('google.cloud.bigquery_storage_v1beta1.BigQueryStorageClient')
    def test_arguments(self, mock_Client, mock_BigQueryStorageClient):
        query_str = "select * from users"
        query = Query(query_str)
        pbq = PBQ(query)
        self.assertEqual(pbq.query, query_str)
