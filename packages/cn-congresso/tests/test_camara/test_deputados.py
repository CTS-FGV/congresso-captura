import pytest
from unittest import TestCase
from congresso.camara.deputados import DeputadosClient


class Tests(TestCase):

    @pytest.fixture()
    def deputados(self):
            return DeputadosClient()

    def test_obter_deputados(self):

        deputados = DeputadosClient()

        self.assertIsInstance(deputados.obter_deputados(), str)