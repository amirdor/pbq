#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pbq` package."""


import unittest
from unittest import mock

from pbq import Query


class TestQuery(unittest.TestCase):
    """Tests for `pbq` package."""

    @mock.patch('google.cloud.bigquery.Client')
    def test_query_format(self, mock_Client):
        query_file = 'files/sample_query.sql'
        query = Query.read_file(query_file)
        q_file = open(query_file, 'r')
        _query = q_file.read()
        self.assertEqual(query.query, _query)

    @mock.patch('google.cloud.bigquery.Client')
    def test_query_format_with_variables(self, mock_Client):
        import json
        with open('files/init.json') as json_file:
            data = json.load(json_file)
        query = Query.read_file('files/variable_query.sql', parameters=data)
        q_file = open('files/sample_query.sql', 'r')
        _query = q_file.read()
        self.assertEqual(query.query, _query)

    @mock.patch('google.cloud.bigquery.Client')
    def test_query_format_with_variables_as_dict(self, mock_Client):
        query = Query.read_file('files/variable_query.sql', parameters={'until_day': 1})
        q_file = open('files/sample_query.sql', 'r')
        _query = q_file.read()
        self.assertEqual(query.query, _query)

    @mock.patch('google.cloud.bigquery.Client')
    def test_query_format_with_missing_values(self, mock_Client):
        with self.assertRaises(ValueError):
            Query.read_file('files/variable_query.sql')
