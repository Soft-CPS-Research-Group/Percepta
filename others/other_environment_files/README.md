O segredo para o source_mapping não se tornar um caos é perceber que, embora os protocolos mudem (AMQP, HTTP, MQTT), o conceito de endereçamento é quase sempre o mesmo.

Podes organizar o source_mapping seguindo uma hierarquia de Escopo (Scope). Existem apenas três níveis onde um dado pode "viver":

1. Nível de Ambiente (Global)
É o caso do teu i-charging atual. Uma única "porta de entrada" para tudo o que pertence àquela instalação.

RabbitMQ: Uma Exchange para o ambiente.

MQTT: Um tópico raiz (ex: telemetry/SaoMamede/#).

HTTP: Um único endpoint que devolve um JSON com todos os sensores da casa.

2. Nível de Entidade (Grupo de Parâmetros)
É o caso do teu novo provider i-energy (Shelly). Cada dispositivo físico tem o seu próprio canal.

RabbitMQ: Uma Exchange por Serial Number do dispositivo.

MQTT: Um tópico por dispositivo (ex: devices/Shelly_01/status).

HTTP: Um URL que devolve o estado completo de um carregador específico.

3. Nível de Parâmetro (Granularidade Total)
É o caso da Cleanwatts. Tens de ir buscar cada "fio" individualmente.

HTTP: Um ID ou URL por cada sensor (Potência, Tensão, Corrente).

MQTT: Um tópico por métrica (ex: sensors/fridge/power).


3 níveis são o "Sweet Spot". Cobrem 99% dos casos de uso em IoT/Energia.

Environment: Para agregação massiva (ex: uma exchange por condomínio).
Entity: Para ativos inteligentes (ex: um carregador i-charging ou um inversor PV).
Parameter: Para sensores legados ou APIs REST granulares (ex: Cleanwatts).

GetAddress(target_id) -> Se target_id está em rules, retorna rules[target_id]. Senão, retorna o próprio target_id.