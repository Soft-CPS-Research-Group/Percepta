from datetime import timedelta

def analisar_timestamps(ficheiro_entrada='timestamps_log.txt', ficheiro_saida='timestamps_over_1s.txt'):
    with open(ficheiro_entrada, 'r') as f_in, open(ficheiro_saida, 'w') as f_out:
        for linha in f_in:
            partes = linha.strip().split(',')
            if len(partes) < 4:
                continue  # ignora linhas inválidas

            # O último campo é a diferença temporal, exemplo: "0:00:00.000004"
            tempo_str = partes[-1]
            try:
                h, m, s = tempo_str.split(':')
                segundos = float(s)
                total_segundos = int(h) * 3600 + int(m) * 60 + segundos

                if total_segundos > 1.0:
                    f_out.write(linha)
            except Exception:
                continue  # ignora formatos inesperados

if __name__ == '__main__':
    analisar_timestamps()
