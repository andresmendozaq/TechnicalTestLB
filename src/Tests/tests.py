# Se importan tanto las librerias necesarias como las funciones a testear del archivo TestLB.py

import unittest
from datetime import datetime
from TestLB import num_dias
from TestLB import consulta_jsons

#Se genera una clase para realizar los tests sobre la función num_dias
class test_nDias(unittest.TestCase):

    # test unitario para verificar formato de fecha inicial
    def test_numero_dias_f_inicial_no_texto(self):
        fecha_ini=1000
        fecha_fin="2020-06-01"
        with self.assertRaises(ValueError) as exc:
            num_dias(fecha_ini,fecha_fin)

        self.assertEqual("La fecha inicial debe ser una fecha tipo AAAA-MM-DD",str(exc.exception))

    # test unitario para verificar formato de fecha final
    def test_numero_dias_f_final_no_texto(self):
        fecha_ini="2020-06-01"
        fecha_fin=1000
        with self.assertRaises(ValueError) as exc:
            num_dias(fecha_ini,fecha_fin)

        self.assertEqual("La fecha final debe ser una fecha tipo AAAA-MM-DD",str(exc.exception))

    # test unitario para verificar formato de fecha inicial como tipo fecha #1
    def test_numero_dias_f_inicial_formato_incorrecto_1(self):
        fecha_ini="2020/05/01"
        fecha_fin="2020-06-01"
        with self.assertRaises(ValueError) as exc:
            num_dias(fecha_ini,fecha_fin)

        self.assertEqual("La fecha inicial no está en formato AAAA-MM-DD",str(exc.exception))

    # test unitario para verificar formato de fecha inicial como tipo fecha #2
    def test_numero_dias_f_inicial_formato_incorrecto_2(self):
        fecha_ini="01-02-2020"
        fecha_fin="2020-03-01"
        with self.assertRaises(ValueError) as exc:
            num_dias(fecha_ini,fecha_fin)

        self.assertEqual("La fecha inicial no está en formato AAAA-MM-DD",str(exc.exception))

    # test unitario para verificar formato de fecha final como tipo fecha #1
    def test_numero_dias_f_final_formato_incorrecto(self):
        fecha_ini="2020-05-01"
        fecha_fin="2020/06/01"
        with self.assertRaises(ValueError) as exc:
            num_dias(fecha_ini,fecha_fin)

        self.assertEqual("La fecha final no está en formato AAAA-MM-DD",str(exc.exception))

    # test unitario para verificar formato de fecha final como tipo fecha #2
    def test_numero_dias_f_final_formato_incorrecto_2(self):
        fecha_ini="2020-01-20"
        fecha_fin="01-03-2020"
        with self.assertRaises(ValueError) as exc:
            num_dias(fecha_ini,fecha_fin)

        self.assertEqual("La fecha final no está en formato AAAA-MM-DD",str(exc.exception))

    # test unitario para verificar que la fecha inicial sea anterior a la fecha final
    def test_numero_dias_f_inicial_anterior(self):
        fecha_ini="2020-01-20"
        fecha_fin="2019-01-20"
        with self.assertRaises(ValueError) as exc:
            num_dias(fecha_ini,fecha_fin)

        self.assertEqual("La fecha inicial debe ser anterior a la fecha final",str(exc.exception))

    # test unitario para verificar que la fecha inicial sea anterior al día de hoy
    def test_numero_dias_f_inicial_anterior_hoy(self):
        fecha_ini="2080-01-20"
        fecha_fin="2080-02-20"
        with self.assertRaises(ValueError) as exc:
            num_dias(fecha_ini,fecha_fin)

        self.assertEqual("La fecha inicial debe ser anterior al día de hoy",str(exc.exception))

    # test unitario para verificar que la fecha final sea anterior al día de hoy
    def test_numero_dias_f_final_anterior_hoy(self):
        fecha_ini="2021-01-20"
        fecha_fin="2080-02-20"
        with self.assertRaises(ValueError) as exc:
            num_dias(fecha_ini,fecha_fin)

        self.assertEqual("La fecha final debe ser anterior al día de hoy",str(exc.exception))








#Se genera una clase para realizar los tests sobre la función consulta_jsons
class test_consultas(unittest.TestCase):

    # test unitario para verificar formato de fecha inicial #1
    def test_consultas_f_inicial_no_fecha_1(self):
        fecha="02-15-2020"
        nmdias=2
        with self.assertRaises(ValueError) as exc:
            consulta_jsons(fecha,nmdias)

        self.assertEqual("La fecha inicial debe estar en formato de fecha",str(exc.exception))

    # test unitario para verificar formato de fecha inicial #2
    def test_consultas_f_inicial_no_fecha_2(self):
        fecha="texto"
        nmdias=2
        with self.assertRaises(ValueError) as exc:
            consulta_jsons(fecha,nmdias)

        self.assertEqual("La fecha inicial debe estar en formato de fecha",str(exc.exception))

    # test unitario para verificar formato del número de dias #1
    def test_consultas_Dias_no_entero_1(self):
        fecha=datetime.strptime("2020-05-12", '%Y-%m-%d')
        nmdias=2.5
        with self.assertRaises(ValueError) as exc:
            consulta_jsons(fecha,nmdias)

        self.assertEqual("El número de dias debe ser un número entero",str(exc.exception))

    # test unitario para verificar formato del número de dias #2
    def test_consultas_Dias_no_entero_2(self):
        fecha=datetime.strptime("2020-05-12", '%Y-%m-%d')
        nmdias="valor"
        with self.assertRaises(ValueError) as exc:
            consulta_jsons(fecha,nmdias)

        self.assertEqual("El número de dias debe ser un número entero",str(exc.exception))

    # test unitario para verificar que el número de días sea igual o mayor a 1
    def test_consultas_Dias_menor_a_1(self):
        fecha=datetime.strptime("2020-05-12", '%Y-%m-%d')
        nmdias=0
        with self.assertRaises(ValueError) as exc:
            consulta_jsons(fecha,nmdias)

        self.assertEqual("El número de dias debe ser mayor a cero",str(exc.exception))

    # test unitario para verificar que el fecha inicial  es anterior al día de hoy
    def test_consultas_Fecha_anterior_hoy(self):
        fecha=datetime.strptime("2080-05-12", '%Y-%m-%d')
        nmdias=2
        with self.assertRaises(ValueError) as exc:
            consulta_jsons(fecha,nmdias)

        self.assertEqual("La fecha inicial debe ser anterior al día de hoy",str(exc.exception))

    # test unitario para verificar que la fecha final estimada es anterior al día de hoy
    def test_consultas_Fecha_anterior_hoy(self):
        fecha=datetime.strptime("2020-05-12", '%Y-%m-%d')
        nmdias=20000
        with self.assertRaises(ValueError) as exc:
            consulta_jsons(fecha,nmdias)

        self.assertEqual("La fecha final estimada debe ser anterior al día de hoy",str(exc.exception))