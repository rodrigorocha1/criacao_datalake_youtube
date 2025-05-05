from enum import Enum


class Camada(Enum):
    Bronze = 'bronze'
    Prata = 'prata'
    Ouro = 'ouro'
    Depara = 'depara'


if __name__ == "__main__":
    a = Camada.Bronze.value
    print(type(Camada.Bronze.value))
    print(a)

    def teste(c: Camada):
        print(c)

    teste(Camada.Depara.value)
