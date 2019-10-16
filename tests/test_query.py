#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pbq` package."""


import unittest
from unittest import mock

from pbq import Query
import os
import json


class TestQuery(unittest.TestCase):
    """Tests for `pbq` package."""

    @mock.patch('google.cloud.bigquery.Client')
    def test_query_format(self, mock_Client):
        cwd = os.getcwd()
        query_file = '{}/tests/files/sample_query.sql'.format(cwd)
        query = Query.read_file(query_file)
        q_file = open(query_file, 'r')
        _query = q_file.read()
        self.assertEqual(query.query, _query)

    @mock.patch('google.cloud.bigquery.Client')
    def test_query_format_with_variables(self, mock_Client):
        cwd = os.getcwd()
        with open('{}/tests/files/init.json'.format(cwd)) as json_file:
            data = json.load(json_file)
        query = Query.read_file('{}/tests/files/variable_query.sql'.format(cwd), parameters=data)
        q_file = open('{}/tests/files/sample_query.sql'.format(cwd), 'r')
        _query = q_file.read()
        self.assertEqual(query.query, _query)

    @mock.patch('google.cloud.bigquery.Client')
    def test_query_format_with_variables_as_dict(self, mock_Client):
        cwd = os.getcwd()
        query = Query.read_file('{}/tests/files/variable_query.sql'.format(cwd), parameters={'until_day': 1})
        q_file = open('{}/tests/files/sample_query.sql'.format(cwd), 'r')
        _query = q_file.read()
        self.assertEqual(query.query, _query)

    @mock.patch('google.cloud.bigquery.Client')
    def test_query_format_with_missing_values(self, mock_Client):
        cwd = os.getcwd()
        with self.assertRaises(ValueError):
            Query.read_file('{}/tests/files/variable_query.sql'.format(cwd))
