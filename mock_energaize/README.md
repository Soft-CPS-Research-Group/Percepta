# Mock do EnergAIze

Mini serviço que simula o EnergAIze, para testar o Percepta sem o modelo real.

Recebe o pedido que o Percepta faz ao modelo (`POST /inference`), imprime-o no terminal e responde com um dicionário
de decisões. Por defeito responde `{}` (sem decisões), por isso nada é enviado aos Target Systems.

## Endpoints

| Método | Caminho      | O que faz                                                              |
|--------|--------------|------------------------------------------------------------------------|
| POST   | `/inference` | Imprime o pedido e devolve as decisões (`DECISIONS`, ou `{}`).         |
| GET    | `/health`    | Devolve `{"status": "ok"}`. Serve para ver se o serviço está de pé.   |

## Como correr (Docker)

A partir da raiz do projeto:

```
docker build -t mock-energaize ./mock_energaize
docker run --rm -it --init -p 8000:8000 mock-energaize
```

O serviço fica em `http://localhost:8000`, que é o endereço que a configuração do Percepta já usa
(`/inference`). Os pedidos recebidos aparecem no terminal onde o contentor corre. Para parar: `Ctrl+C`.

Porque `-it --init`: o Python corre como processo principal (PID 1) do contentor, e sem estas opções o `Ctrl+C` pode
ser ignorado.
- `-it` liga o teu terminal ao contentor, por isso o `Ctrl+C` chega ao processo.
- `--init` põe um pequeno processo à frente que reencaminha os sinais (`Ctrl+C` e `docker stop`) e fecha o contentor
  como deve ser.
- `--rm` apaga o contentor quando pára.

Se mesmo assim não parar, abre outro terminal e faz `docker ps` e depois `docker stop <id ou nome>`. Pode demorar cerca
de 10 s, que é o tempo que o Docker espera antes de forçar o fim.

Se alterares o `server.py`, volta a fazer o `docker build`, porque a imagem antiga não vê as alterações.

## Enviar decisões (variável `DECISIONS`)

`DECISIONS` é um dicionário JSON `id da entidade -> potência em kW`. Um valor negativo descarrega uma bateria.

PowerShell (aspas simples por fora):
```
docker run --rm -it --init -p 8000:8000 -e 'DECISIONS={"B02": -2.5}' mock-energaize
```

Linux / macOS / WSL:
```
docker run --rm -it --init -p 8000:8000 -e DECISIONS='{"B02": -2.5}' mock-energaize
```

Cuidados:
- O mock devolve **sempre** a mesma resposta, a cada pedido (por isso, a cada período). A bateria é atuada em todos
  os períodos até parares o mock.
- As entidades têm de existir no ambiente (`environment_files/environments.json`) e ter um atuador de potência.
  Entidades desconhecidas são ignoradas com um aviso.
- Se o JSON for inválido, o mock regista o erro e devolve `{}`.

## Como correr sem Docker

```
pip install "aiohttp~=3.13.3"
python mock_energaize/server.py
```

Com decisões (Linux / macOS / WSL):
```
DECISIONS='{"B02": -2.5}' python mock_energaize/server.py
```

A porta é `8000`, e muda-se com a variável `PORT` (por exemplo `PORT=8001`).

## Testar o mock à mão

```
curl -X POST http://localhost:8000/inference -H "Content-Type: application/json" -d '{"teste": 1}'
curl http://localhost:8000/health
```
