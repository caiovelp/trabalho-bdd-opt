import time
from main import query_1, query_2, query_3, query_4, query_5, query_6, query_7

def main():
    query_funcs = [query_1, query_2, query_3, query_4, query_5, query_6, query_7]
    query_names = [
        "Crimes por tipo e segmento em IGUATEMI (2016)",
        "Crimes por tipo e segmento em IGUATEMI (2006-2016)",
        "Roubo de celular/carro em SANTA EFIGÊNIA (2015)",
        "Crimes por tipo em ruas de mão única (2012)",
        "Roubo de carro/celular em todos os segmentos (2017)",
        "Segmentos com maior índice de criminalidade (Nov 2010)",
        "Segmentos com maior índice de criminalidade (fins de semana 2018)"
    ]

    n_runs = 5
    avg_times = []
    for func in query_funcs:
        times = []
        for _ in range(n_runs):
            start = time.time()
            _ = func()
            times.append(time.time() - start)
        avg_times.append(sum(times) / n_runs)

    print("Query\t\t\t\t\t\t\t\t\tTempo médio (s)")
    for name, t in zip(query_names, avg_times):
        print(f"{name:60} {t:.3f}")

if __name__ == '__main__':
    main()