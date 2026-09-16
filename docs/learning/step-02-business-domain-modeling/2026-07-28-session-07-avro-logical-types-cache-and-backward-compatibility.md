# Guia de Aprendizado — Etapa 2: Modelagem de Domínio de Negócio (Business Domain Modeling) — Sessão 07: Refinamento de Schemas Avro com Tipos Lógicos, Cache LRU e Compatibilidade Backward

---

## 1. Resumo Executivo & Visão Geral

A **Sessão 07** conclui o refinamento avançado da **Etapa 2 (Modelagem de Domínio de Negócio)** do projeto [Marketplace Sandbox](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/README.md). Enquanto a Sessão 06 estabeleceu os fundamentos dos Contratos de Dados (*Data Contracts*) utilizando **Apache Avro** e **Confluent Schema Registry**, esta sessão concentrou-se na elevação da maturidade de produção através de três pilares fundamentais: **Tipos Lógicos Avro**, **Cache em Memória de Schemas** e **Imposição de Compatibilidade BACKWARD**.

Em pipelines de dados financeiros e de e-commerce, o tratamento de precisão monetária e instantes temporais é crítico. Representar valores monetários como `float` ou `double` introduz erros de arredondamento inerentes ao padrão IEEE 754 (ex: `0.1 + 0.2 = 0.30000000000000004`), inaceitáveis para auditorias contábeis. Da mesma forma, representar timestamps como strings sem fuso horário dificulta consultas analíticas e agregações no Spark SQL e Trino.

Para resolver essas deficiências e otimizar a performance de I/O em tempo de execução, os schemas `.avsc` foram refatorados para utilizar os tipos lógicos `timestamp-millis` e `decimal(18,4)`. Além disso, a engine de serialização e deserialização foi aprimorada com conversões automáticas e um mecanismo de **Cache em Memória** no [ingestion/schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schema_registry.py) para zerar chamadas HTTP redundantes ao Schema Registry. Por fim, instituiu-se a regra rigorosa de compatibilidade **`BACKWARD`** no script de automação [scripts/register_schemas.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/register_schemas.py).

### Objetivos Alcançados na Sessão 07

- **Refatoração dos Schemas (.avsc):** Atualização dos 6 contratos em [ingestion/schemas/avro/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/) incorporando `logicalType: timestamp-millis` para datas e `logicalType: decimal` com precisão 18 e escala 4 (`decimal(18,4)`) para campos monetários.
- **Transformação Transparente de Tipos Python:** Aprimoramento do módulo [ingestion/serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/serialization.py) com pré/pós-processamento automático convertendo objetos Python `datetime` (UTC) e `Decimal` para os tipos nativos interpretados pelo `fastavro`.
- **Cache de Schemas por ID em Memória:** Implementação do atributo `_schema_id_cache` em `SchemaRegistryManager`, eliminando requisições HTTP adicionais em mensagens com o mesmo `schema_id`.
- **Governança Estrita com Compatibilidade BACKWARD:** Configuração e validação da política de compatibilidade `BACKWARD` no Confluent Schema Registry durante o registro automatizado via CLI.
- **Validação de Qualidade Integrada:** Suíte de testes atualizada em [tests/test_serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_serialization.py) e [tests/test_schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_schema_registry.py), garantindo 100% de aprovação no `pytest`.

---

## 2. Arquitetura & Decisões de Design

A arquitetura refatorada introduz uma camada de abstração de tipos lógicos e otimização de I/O entre os microserviços produtores/consumidores e o repositório de governança.

### Fluxo de Dados Otimizado com Cache e Tipos Lógicos

```mermaid
flowchart TD
    subgraph PythonApplication ["Produtor / Consumidor Python"]
        PayloadPy["Payload nativo (datetime UTC, Decimal)"]
        SerDe["ingestion/serialization.py\n(AvroSerializerDeserializer)"]
    end

    subgraph LogicalTypesEngine ["Camada de Pré/Pós Processamento"]
        PreProc["_preprocess_payload()\n(datetime -> Epoch ms, Decimal -> Decimal)"]
        PostProc["_postprocess_payload()\n(Epoch ms -> datetime UTC, Decimal -> Decimal)"]
    end

    subgraph CacheLayer ["Gestão de Schemas & In-Memory Cache"]
        SRManager["ingestion/schema_registry.py\n(SchemaRegistryManager)"]
        CacheMemory["_schema_id_cache (dict: schema_id -> parsed schema)"]
        LocalRegistry["Confluent Schema Registry\n(http://localhost:8081)\n[Policy: BACKWARD]"]
    end

    subgraph KafkaStream ["Barramento de Eventos Kafka"]
        WireFormat["Confluent Wire Format\n(Magic Byte 0x00 + Schema ID + Binary)"]
    end

    PayloadPy --> PreProc
    PreProc -->|Payload Preparado| SerDe
    SerDe <-->|1. Busca Schema no Cache Local| CacheMemory
    CacheMemory -- Hit -->|Retorna Schema em Memória| SerDe
    CacheMemory -- Miss -->|2. HTTP GET /schemas/ids/ID| SRManager
    SRManager <-->|HTTP REST API| LocalRegistry
    SRManager -->|Atualiza Cache Local| CacheMemory
    SerDe -->|3. Serializa em Bytes com FastAvro| WireFormat
    WireFormat -->|4. Consumo Binário| SerDe
    SerDe --> PostProc
    PostProc -->|Payload Reconstruído com Tipos Python| PayloadPy
```

### Mapeamento de Tipos Lógicos nos Eventos de Negócio

A tabela a seguir resume como os dados foram refinados nos esquemas do projeto:

| Campo do Evento | Eventos Afetados | Tipo Físico Avro | Tipo Lógico Avro | Tipo Nativo Python | Motivação da Escolha |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `timestamp` | Todos os 6 eventos | `long` | `timestamp-millis` | `datetime` (UTC) | Padronização temporal uniforme em milissegundos UTC sem ambiguidade de fuso. |
| `unit_price`, `subtotal`, `total_amount` | `order_creation`, `payment_updates`, `cart_events`, `catalog_changes` | `bytes` | `decimal` (prec=18, scale=4) | `Decimal` | Garantia de precisão financeira exata de até 4 casas decimais para arredondamento contábil. |
| `discount_amount`, `shipping_fee` | `order_creation`, `payment_updates` | `bytes` | `decimal` (prec=18, scale=4) | `Decimal` | Evita perdas em operações de desconto e cálculos de frete. |
| `amount` | `payment_updates` | `bytes` | `decimal` (prec=18, scale=4) | `Decimal` | Rigor auditável para gateways de pagamento. |

---

## 3. Conceitos Fundamentais

### 3.1 Avro Logical Types

No Apache Avro, um **Tipo Lógico** (*Logical Type*) é uma anotação estendida sobre um tipo primitivo ou complexo físico que especifica como os dados devem ser interpretados semanticamente pelas aplicações.

1. **`timestamp-millis`:**
   - **Tipo Físico:** `long` (inteiro de 64 bits).
   - **Semântica:** Representa a quantidade de milissegundos decorridos desde a época Unix (*Unix Epoch*: 1 de Janeiro de 1970 00:00:00 UTC).
   - **Vantagem:** Ocupa apenas 8 bytes na transmissão binária (muito menor que uma string ISO-8601 de 24+ bytes) e elimina variações de parsing regional.

2. **`decimal(precision, scale)`:**
   - **Tipo Físico:** `bytes` ou `fixed`.
   - **Semântica:** Representa um número decimal de precisão arbitrária. A `precision` define o número total de dígitos significativos (ex: 18), e a `scale` define a quantidade de dígitos à direita da vírgula decimal (ex: 4).
   - **Armazenamento:** O valor é codificado em binário usando complemento de dois (*two's complement byte array*). Por exemplo, com escala 4, o valor numérico $125.5000$ é armazenado fisicamente como o inteiro $1255000$.

### 3.2 Cache LRU / In-Memory Schema Caching

Em sistemas de streaming de alta vazão (milhares de eventos por segundo), realizar uma chamada HTTP ao Schema Registry para cada mensagem deserializada criaria um gargalo devastador de latência e I/O de rede.

- **Funcionamento:** Quando a aplicação lê o cabeçalho de 5 bytes do Confluent Wire Format, ela extrai o `schema_id`. O `SchemaRegistryManager` primeiro verifica se a chave `schema_id` existe em seu dicionário interno `_schema_id_cache`.
- **Cache Hit:** O esquema Avro parseado é retornado instantaneamente da memória (latência $< 1\mu s$).
- **Cache Miss:** Uma requisição HTTP `GET /schemas/ids/{id}` é feita ao serviço remoto, e o resultado é armazenado no cache local para todas as requisições subsequentes.

### 3.3 Compatibility Enforcement: `BACKWARD` vs `NONE`

O Confluent Schema Registry atua como o guardião da evolução dos Contratos de Dados. As políticas de compatibilidade regem o que é permitido alterar nos esquemas sem quebrar a infraestrutura downstream.

```text
Evolução de Schemas:
  [Versão 1 (Antiga)]  <--- Consumidor com V2 consegue ler? --->  [Versão 2 (Nova)]
```

- **Compatibilidade `BACKWARD` (Default Recomendado):**
  - **Regra:** Um novo esquema (V2) é compatível com o esquema anterior (V1) se os dados gravados com V1 puderem ser lidos por consumidores atualizados para o esquema V2.
  - **Permite:** Deletar campos existentes ou adicionar novos campos **opcionais** (que possuam um valor `"default"` especificado).
  - **Proíbe:** Adicionar campos obrigatórios sem valor padrão ou alterar tipos de campos incompatíveis.
  - **Impacto:** Garante que os consumidores possam ser atualizados *antes* dos produtores sem interromper a leitura de mensagens históricas ou em trânsito.

- **Compatibilidade `NONE`:**
  - Desativa completamente as verificações no Schema Registry. Qualquer alteração estrutural é aceita, o que frequentemente resulta em falhas em tempo de execução (*runtime crashes*) e corrupção de dados na Camada Bronze do Data Lakehouse.

---

## 4. Implementação Passo a Passo

A refatoração e implementação da Sessão 07 transcorreram em 5 passos principais:

### Passo 1: Atualização dos Schemas `.avsc` com Tipos Lógicos

Em todos os 6 arquivos em [ingestion/schemas/avro/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/), as definições de campos temporais e monetários foram atualizadas. Por exemplo, em [order_creation.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/order_creation.avsc):

```json
{
  "name": "timestamp",
  "type": { "type": "long", "logicalType": "timestamp-millis" },
  "doc": "Milliseconds since UTC Unix epoch"
},
{
  "name": "total_amount",
  "type": { "type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 4 },
  "doc": "Monetary value represented as fixed-precision decimal (precision 18, scale 4)"
}
```

### Passo 2: Pré e Pós-Processamento Transparente em `serialization.py`

Desenvolveram-se os métodos recursivos de pré-processamento (`_preprocess_payload` e `_preprocess_value`) e pós-processamento (`_postprocess_payload` e `_postprocess_value`) no [ingestion/serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/serialization.py):

1. **Pré-processamento (antes de serializar/validar):**
   - Converte objetos `datetime` do Python em inteiros de milissegundos Epoch (`int(dt.timestamp() * 1000)`), garantindo fuso horário UTC.
   - Converte valores numéricos ou strings de moeda em objetos `Decimal` nativos do Python.
2. **Pós-processamento (após deserializar):**
   - Converte inteiros de milissegundos recebidos em objetos Python `datetime` com `tzinfo=UTC`.
   - Mantém valores decimais como `Decimal`, evitando a conversão indesejada para `float`.

### Passo 3: Implementação do Cache de Schemas por ID em `schema_registry.py`

No arquivo [ingestion/schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schema_registry.py):

- Inicialização do atributo `self._schema_id_cache: dict[int, dict[str, Any]] = {}` no construtor de `SchemaRegistryManager`.
- Consulta primária no cache em `get_schema_by_id(schema_id)` antes de realizar a requisição HTTP `GET /schemas/ids/{id}`.
- Povoamento do cache durante chamadas com sucesso em `register_schema` e `get_latest_schema`.
- Adição do método helper `clear_schema_cache()` para fins de manutenção e testes unitários.

### Passo 4: Imposição da Regra de Compatibilidade `BACKWARD` na Automação CLI

No script [scripts/register_schemas.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/register_schemas.py), atualizou-se a chamada de registro para aplicar explicitamente a política `BACKWARD` para todos os *subjects* antes do envio do conteúdo do esquema:

```python
results = manager.register_all_schemas_in_dir(schemas_dir, compatibility_level="BACKWARD")
```

### Passo 5: Suíte de Testes e Validação Pytest

Os testes em [tests/test_serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_serialization.py) e [tests/test_schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_schema_registry.py) foram expandidos para testar:
- Validação e roundtrip (serialização -> deserialização) de objetos `datetime` e `Decimal`.
- Verificação do reuso de cache sem chamadas HTTP redundantes.
- Testes de falha de compatibilidade ao tentar registrar schemas incompatíveis.

---

## 5. Análise de Código e Trechos Chave

### 5.1 Pré-Processamento de Tipos Lógicos ([ingestion/serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/serialization.py#L86-L114))

O trecho abaixo exemplifica como a engine intercepta os campos declarados com `logicalType` no esquema Avro e efetua a coerção de tipos Python antes da validação estrita com `fastavro`:

```python
def _preprocess_value(self, val: Any, type_def: Any) -> Any:
    """Preprocess a single field value based on its Avro field schema definition."""
    if val is None:
        return None

    logical_type, rec_schema, arr_items = self._extract_field_info(type_def)

    # Coerção de datas/timestamps para inteiro de milissegundos Epoch
    if logical_type == "timestamp-millis":
        if isinstance(val, datetime):
            dt = val if val.tzinfo is not None else val.replace(tzinfo=UTC)
            return int(dt.timestamp() * 1000)
        elif isinstance(val, str):
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return int(dt.timestamp() * 1000)
        elif isinstance(val, (int, float)):
            return int(val)
            
    # Coerção de valores monetários para Decimal do Python
    elif logical_type == "decimal":
        if isinstance(val, Decimal):
            return val
        elif isinstance(val, (float, int, str)):
            return Decimal(str(val))
            
    # Suporte recursivo para registros aninhados e arrays de objetos
    elif rec_schema and isinstance(val, dict):
        return self._preprocess_payload(val, rec_schema)
    elif arr_items and isinstance(val, list):
        return [self._preprocess_value(item, arr_items) for item in val]

    return val
```

### 5.2 Cache In-Memory de Schemas por ID ([ingestion/schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schema_registry.py#L233-L271))

O método `get_schema_by_id` exemplifica a estratégia de cache *read-through* em memória para desonerar a API REST do Schema Registry:

```python
def get_schema_by_id(self, schema_id: int) -> dict[str, Any]:
    """Fetch a schema by its global unique Schema ID.

    Checks the in-memory schema cache first. On cache miss, queries the Schema Registry endpoint
    and stores the resulting schema dictionary in the cache.
    """
    # 1. Verificação Primária em Memória (Hit Instantâneo)
    if schema_id in self._schema_id_cache:
        logger.debug(
            "Schema ID %d retrieved from in-memory cache.",
            schema_id,
            extra={"schema_id": schema_id},
        )
        return self._schema_id_cache[schema_id]

    # 2. Cache Miss: Consulta HTTP ao Schema Registry
    endpoint = f"/schemas/ids/{schema_id}"
    response = self._request_with_retry("GET", endpoint)

    if response.status_code == 404:
        raise SchemaNotFoundError(f"Schema ID {schema_id} not found in Schema Registry.")

    if response.status_code == 200:
        data = response.json()
        schema_dict = json.loads(data["schema"])
        # 3. Povoamento do Cache em Memória
        self._schema_id_cache[schema_id] = schema_dict
        return schema_dict

    raise SchemaRegistryError(f"Failed to fetch schema ID {schema_id} (HTTP {response.status_code}): {response.text}")
```

### 5.3 Imposição da Regra de Compatibilidade `BACKWARD` ([ingestion/schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schema_registry.py#L311-L359))

O método `set_compatibility_level` assegura que a política global ou por *subject* seja configurada antes do envio de novos esquemas:

```python
def set_compatibility_level(self, compatibility_level: str, subject: str | None = None) -> bool:
    """Set the compatibility mode globally or for a specific subject."""
    level_upper = compatibility_level.upper()
    if level_upper not in VALID_COMPATIBILITY_LEVELS:
        raise ValueError(f"Invalid compatibility level '{compatibility_level}'.")

    endpoint = f"/config/{subject}" if subject else "/config"
    payload = {"compatibility": level_upper}

    response = self._request_with_retry("PUT", endpoint, json_data=payload)

    if response.status_code == 200:
        logger.info("Successfully set compatibility level to '%s' for subject '%s'.", level_upper, subject)
        return True

    raise SchemaRegistryError(f"Failed to set compatibility level to '{level_upper}': {response.text}")
```

---

## 6. Desafios Enfrentados e Soluções

### Desafio: Tratamento de Incompatibilidade HTTP 409 durante Evolução de Schemas no Sandbox

Durante o desenvolvimento e testes no ambiente sandbox local, ao tentar registrar uma versão modificada de um schema Avro (por exemplo, alterando o tipo de um campo existente de `string` para `int`), o Confluent Schema Registry retornava um erro `HTTP 409 Conflict` (ou `HTTP 422 Unprocessable Entity`), bloqueando a execução da automação.

#### Causa Raiz
Com a política de compatibilidade ativada como `BACKWARD`, o Schema Registry compara o novo schema com a última versão registrada no *subject*. Se a modificação quebrar consumidores existentes que utilizam o contrato anterior, o Schema Registry rejeita o registro para proteger o barramento.

#### Solução Aplicada
1. **Verificação Prévia de Compatibilidade:** Implementou-se o método `check_compatibility(subject, schema)` em `SchemaRegistryManager`, que realiza uma chamada `POST /compatibility/subjects/{subject}/versions/latest` antes do registro final.
2. **Exceções Estruturadas:** Criação da classe `SchemaCompatibilityError` para capturar falhas de compatibilidade de forma explícita, fornecendo logs amigáveis com o motivo do conflito em vez de falhas genéricas de conexão HTTP.
3. **Evolução Segura nos Schemas `.avsc`:** Adoção das boas práticas Avro para evolução de esquemas: novos campos devem sempre conter um bloco `"default"` (ex: `"default": null`), permitindo que leitores antigos interpretem novos eventos sem exceções.

---

## 7. Trade-offs de Engenharia & Alternativas Consideradas

### 7.1 Formato de Timestamps: Long (Millis) vs String ISO-8601 vs Long (Micros)

| Abordagem | Tamanho no Payload Binário | Velocidade de Parsing | Compatibilidade com Engines (Spark/Trino) | Decisão & Justificativa |
| :--- | :--- | :--- | :--- | :--- |
| **`long` (`timestamp-millis`)** | 8 bytes | Alta (Operação matemática direta) | **Nativa e excelente** (padrão em Spark SQL) | **Adotado**. Equilíbrio ideal entre compacidade binária e suporte nativo nas engines de Data Lakehouse. |
| **`string` (ISO-8601 UTC)** | ~24-35 bytes | Baixa (Requer regex/string parsing) | Média (Exige funções `to_timestamp` no SQL) | Descartado devido ao overhead de armazenamento binário e custo de parsing em grandes volumes. |
| **`long` (`timestamp-micros`)** | 8 bytes | Alta | Boa | Descartado. A precisão em milissegundos é suficiente para eventos de e-commerce e tem maior compatibilidade. |

### 7.2 Valores Monetários: Decimal vs Float/Double vs Int (Centavos)

| Abordagem | Precisão Financeira | Complexidade de Schema | Risco de Arredondamento | Decisão & Justificativa |
| :--- | :--- | :--- | :--- | :--- |
| **`decimal(18,4)` (bytes)** | **Exata / Absoluta** | Média (Exige declaração de precisão/escala) | **Zero** | **Adotado**. Padrão de indústria para moedas e taxas no e-commerce, suportando frações de centavos (ex: $0.0015$). |
| **`double` / `float`** | Aproximada (IEEE 754) | Baixa (Tipo primitivo direto) | **Alto** (Pode gerar imprecisões no somatório de pedidos) | Descartado. Inaceitável para governança financeira. |
| **`long` (Centavos inteiros)** | Exata | Baixa | Baixo para centavos, alto para frações de centavos | Descartado. Não suporta moedas com taxas fracionárias ou precificação dinâmica com 4 casas decimais. |

---

## 8. Boas Práticas e Padrões Recomendados

1. **Sempre Especifique Valores Padrão (`default`) em Novos Campos:**
   Para manter a compatibilidade `BACKWARD`, qualquer novo campo adicionado a um schema Avro existente **deve** ser opcional (usando unions `["null", "type"]`) ou possuir um valor `"default"`.

2. **Evite Conversões Implícitas de Ponto Flutuante:**
   Em Python, ao instanciar objetos `Decimal` para serialização Avro, sempre passe os valores como strings (ex: `Decimal("125.5000")`) e nunca como `float` (ex: `Decimal(125.50)`), prevenindo contaminação por imprecisão do binário de ponto flutuante.

3. **Padronize Todos os Timestamps em UTC:**
   Nunca serialize objetos `datetime` "naive" (sem indicação de fuso horário). Garanta que os objetos possuam `tzinfo=UTC` antes de converter para milissegundos Epoch.

4. **Gerencie o Ciclo de Vida do Cache Local:**
   Em aplicações de longa duração (*long-running worker daemons*), inclua estratégias para invalidar ou atualizar o cache de schemas (`clear_schema_cache()`) caso ocorram atualizações dinâmicas de esquemas em ambiente de produção.

---

## 9. Perspectiva da Indústria e Casos Reais

- **Plataformas de E-Commerce & Fintechs (Mercado Livre, Stripe, Nubank):**
  Empresas que processam milhões de transações diárias adotam esquemas Avro com tipos lógicos `decimal` rigorosamente validados na borda de ingestão. Isso impede que bugs de arredondamento nos microserviços se propaguem para a contabilidade ou para os relatórios fiscais do Data Warehouse.

- **Arquiteturas Lakehouse com Apache Iceberg e Delta Lake:**
  Engines modernas como **Apache Spark**, **Trino** e **Flink** possuem suporte nativo para ler payloads Avro com `timestamp-millis` e `decimal`, convertendo-os diretamente em colunas `TIMESTAMP` e `DECIMAL(18,4)` no formato Parquet sem necessidade de casts manuais em consultas SQL.

- **Governança CI/CD de Contratos de Dados (Schema Registry CLI):**
  O padrão de incluir o registro automatizado com checagem de compatibilidade `BACKWARD` em pipelines de CI/CD (GitHub Actions / GitLab CI) é a prática recomendada pela Confluent para evitar que Pull Requests que alterem esquemas quebrem os consumidores em produção.

---

## 10. Exercícios Práticos e Próximos Passos

### Exercícios Práticos

1. **Simulação de Quebra de Compatibilidade `BACKWARD`:**
   Crie uma cópia temporária do arquivo [order_creation.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/order_creation.avsc) e adicione um novo campo obrigatório sem valor padrão:
   ```json
   { "name": "mandatory_field", "type": "string" }
   ```
   Execute o script `python scripts/register_schemas.py` com o Schema Registry rodando e observe o erro de rejeição de compatibilidade retornado pelo serviço.

2. **Validação do Cache em Memória com Pytest:**
   Abra o arquivo [tests/test_schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_schema_registry.py) e adicione um teste unitário utilizando `unittest.mock` para verificar que a chamada `get_schema_by_id(101)` invoca o método HTTP apenas na primeira vez, e que a segunda chamada com a mesma chave lê diretamente do cache `_schema_id_cache`.

### Próximos Passos

Com a conclusão bem-sucedida da **Etapa 2 (Modelagem de Domínio de Negócio)**, o projeto está 100% pronto para avançar para a **Etapa 3 do Roadmap (Ingestão de Dados & Barramento Streaming)**:
- Configuração e desenvolvimento dos **Kafka Producers em Python** para envio dos 6 eventos de negócio ao cluster Kafka local.
- Integração da engine de serialização Avro com o cliente Kafka (`confluent-kafka` / `kafka-python`).
- Validação da recepção dos dados binários no barramento e consumo inicial via Spark Streaming na Camada Bronze.
