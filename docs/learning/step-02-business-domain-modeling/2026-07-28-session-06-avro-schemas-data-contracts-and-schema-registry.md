# Guia de Aprendizado — Etapa 2: Modelagem de Domínio de Negócio (Business Domain Modeling) — Sessão 06: Esquemas Avro, Contratos de Dados e Confluent Schema Registry

---

## 1. Resumo Executivo & Visão Geral

A **Sessão 06** consolida a fase final da **Etapa 2 (Modelagem de Domínio de Negócio)** do projeto [Marketplace Sandbox](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/README.md), integrando a especificação funcional de eventos definida na Sessão 05 à infraestrutura técnica de **Contratos de Dados (Data Contracts)** em nível de produção.

Em arquiteturas modernas impulsionadas por eventos (*Event-Driven Architectures - EDA*), o desacoplamento entre produtores e consumidores traz o risco de *schema drift* (desvio inadvertido da estrutura dos dados). Sem uma camada rigorosa de governança, alterações sutis em um payload JSON operado por um microserviço podem propagar erros em cascata nos pipelines de ingestão *downstream*, corrompendo a Camada Bronze do Data Lakehouse.

Para mitigar esse risco, esta sessão implementou uma solução completa baseada em **Apache Avro** e no **Confluent Schema Registry**. Foram traduzidos os 6 eventos de negócio do ecossistema do marketplace em esquemas formais `.avsc`, construídos módulos em Python para gestão de registro e serialização binária com o padrão **Confluent Wire Format** (cabeçalho de 5 bytes), desenvolvido um script CLI para registro automatizado e criada uma suíte robusta de testes unitários e de integração com `pytest`.

### Objetivos Alcançados na Sessão 06

- **Codificação dos Schemas Avro (.avsc):** Criação dos esquemas estritos para `order_creation`, `payment_updates`, `cart_events`, `stock_updates`, `user_navigation` e `catalog_changes` em [ingestion/schemas/avro/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/).
- **Gerenciador HTTP do Schema Registry:** Desenvolvimento do cliente programmatico [ingestion/schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schema_registry.py) com suporte a *retries* exponenciais, tratamento de erros e checagem de compatibilidade.
- **Engine de Serialização Binária:** Construção do módulo [ingestion/serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/serialization.py), suportando validação estrita com `fastavro` e empacotamento do cabeçalho binário do Confluent Wire Format.
- **Automação via Script CLI:** Desenvolvimento do script [scripts/register_schemas.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/register_schemas.py) para publicar automaticamente todos os esquemas no Confluent Schema Registry local (`localhost:8081`).
- **Garantia de Qualidade via Pytest:** Implementação dos arquivos de testes [tests/test_schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_schema_registry.py) e [tests/test_serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_serialization.py), obtendo 100% de aprovação.
- **Consolidação da Documentação Técnica:** Atualização completa do documento de especificação formal em [docs/business_domain_modeling.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/business_domain_modeling.md).

---

## 2. Arquitetura & Decisões de Design

A arquitetura de contratos de dados do **Marketplace Sandbox** estabelece uma barreira de proteção na borda da plataforma de dados. O fluxo garante que nenhum dado seja publicado em tópicos do Kafka ou persistido na arquitetura Medalhão sem validação prévia de esquema.

### Fluxo de Dados & Validação de Contratos

```mermaid
flowchart TD
    subgraph Producers ["Microserviços Produtores (Domínio de Negócio)"]
        OrderSvc["Checkout Service"]
        PaySvc["Payment Gateway"]
        CartSvc["Cart Telemetry"]
        StockSvc["Inventory Service"]
        NavSvc["Clickstream Engine"]
        CatSvc["Catalog Manager"]
    end

    subgraph DataContractLayer ["Camada de Governança & Data Contracts"]
        SerDe["ingestion/serialization.py\n(AvroSerializerDeserializer)"]
        SRManager["ingestion/schema_registry.py\n(SchemaRegistryManager)"]
        LocalRegistry["Confluent Schema Registry\n(http://localhost:8081)"]
    end

    subgraph KafkaBroker ["Apache Kafka Event Bus"]
        T_Order["marketplace.orders"]
        T_Pay["marketplace.payments"]
        T_Cart["marketplace.cart"]
        T_Stock["marketplace.inventory"]
        T_Nav["marketplace.navigation"]
        T_Cat["marketplace.catalog"]
    end

    subgraph DownstreamEngine ["Ingestão & Analytics Downstream"]
        SparkIngestion["PySpark Streaming Engine"]
        BronzeMinIO["Medallion Bronze Layer (MinIO Raw Store)"]
    end

    Producers -->|1. Envia Payload Python| SerDe
    SerDe -->|2. Valida Schema estritamente via fastavro| SerDe
    SerDe <-->|3. Obtém ou Registra Schema ID| SRManager
    SRManager <-->|HTTP REST API| LocalRegistry
    SerDe -->|4. Precede Magic Byte 0x00 + Schema ID (5 bytes)| KafkaBroker
    KafkaBroker -->|5. Consome Binário Otimizado| SparkIngestion
    SparkIngestion -->|6. Resolve Schema por ID e Decodifica| BronzeMinIO
```

### Mapeamento de Entidades, Tópicos e Subjects no Schema Registry

O projeto adota uma convenção de nomenclatura estrita para vincular tópicos do Kafka aos *subjects* (assuntos) no Confluent Schema Registry. A estratégia utilizada é a **TopicNameStrategy** padrão da Confluent, onde cada valor enviado ao tópico `<topic-name>` possui um registro de esquema nomeado `<topic-name>-value`.

| Evento de Negócio | Tópico Kafka (`marketplace.<subsystem>`) | Subject no Schema Registry (`<topic>-value`) | Arquivo Avro (.avsc) | Chave de Particionamento |
| :--- | :--- | :--- | :--- | :--- |
| Criacão de Pedidos | `marketplace.orders` | `marketplace.orders-value` | [order_creation.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/order_creation.avsc) | `order_id` |
| Atualizações de Pagamento | `marketplace.payments` | `marketplace.payments-value` | [payment_updates.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/payment_updates.avsc) | `payment_id` |
| Eventos de Carrinho | `marketplace.cart` | `marketplace.cart-value` | [cart_events.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/cart_events.avsc) | `cart_id` |
| Movimentação de Estoque | `marketplace.inventory` | `marketplace.inventory-value` | [stock_updates.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/stock_updates.avsc) | `product_id` |
| Navegação de Usuário | `marketplace.navigation` | `marketplace.navigation-value` | [user_navigation.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/user_navigation.avsc) | `session_id` |
| Alterações de Catálogo | `marketplace.catalog` | `marketplace.catalog-value` | [catalog_changes.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/catalog_changes.avsc) | `product_id` |

---

## 3. Conceitos Fundamentais

### 3.1 Apache Avro
O **Apache Avro** é um sistema de serialização de dados binários independente de linguagem, desenvolvido no âmbito do ecossistema Apache Hadoop. As principais características que o tornam ideal para plataformas de dados orientadas a eventos incluem:

1. **Separação entre Dados e Esquema:** Ao contrário do JSON ou XML, que repetem os nomes das chaves/atributos em cada mensagem individual, os payloads binários do Avro contêm apenas os valores dos campos. O esquema é armazenado separadamente.
2. **Representação JSON dos Esquemas:** Os esquemas Avro são definidos em arquivos com formato JSON legível (`.avsc`), permitindo controle de versão via Git e facilidade de documentação.
3. **Tipagem Forte & Estrutura Completa:** Suporta tipos primitivos (`string`, `int`, `long`, `float`, `double`, `boolean`, `bytes`), tipos complexos (`record`, `array`, `map`, `enum`) e tipos nulos/opcionais através de **Unions** (ex: `["null", "string"]`).

### 3.2 Confluent Schema Registry
O **Confluent Schema Registry** é um serviço centralizado com interface RESTful projetado para armazenar, gerenciar e servir esquemas de eventos em um cluster Kafka. Suas funções cruciais são:

- **Repositório Central de Esquemas:** Mantém um histórico versionado de todas as alterações estruturais sofridas pelos dados.
- **Validação de Compatibilidade:** Impede que um produtor registre ou envie um esquema que quebre os consumidores existentes (habilidade de configurar políticas como `BACKWARD`, `FORWARD`, `FULL` e `NONE`).
- **Atribuição de IDs Globais:** Cada esquema válido recebe um **Schema ID** único (inteiro de 32 bits), permitindo a identificação inequívoca de qualquer mensagem trafegada.

### 3.3 Confluent Wire Format
Para que um consumidor saiba qual esquema utilizar na deserialização de uma mensagem binária sem que o esquema inteiro precise ser enviado em cada registro, a Confluent definiu o **Confluent Wire Format** para o Apache Kafka.

Uma mensagem que segue o Wire Format possui um cabeçalho estrito de **5 bytes** adicionado antes do payload binário Avro:

```text
+--------------+-------------------+--------------------------------------+
| Byte 0       | Bytes 1-4         | Bytes 5...                           |
| Magic Byte   | Schema ID         | Avro Binary Payload                  |
| (0x00)       | (32-bit Integer)  | (schemaless Avro encoded bytes)      |
+--------------+-------------------+--------------------------------------+
```

- **Byte 0 (Magic Byte):** Um byte fixo com valor hex `0x00` que sinaliza que a mensagem utiliza a convenção do Confluent Schema Registry.
- **Bytes 1-4 (Schema ID):** Um inteiro de 32 bits codificado em formato *Big-Endian* (network byte order), representando o id numérico do esquema registrado no Schema Registry.
- **Bytes 5 em diante:** O payload binário Avro puro codificado de forma *schemaless*.

### 3.4 Data Contracts (Contratos de Dados)
Um **Contrato de Dados** é um acordo formal, auditável e vinculativo entre as equipes produtoras de software e as equipes consumidoras de dados. Ele especifica:
- A estrutura sintática dos dados (esquema Avro/JSON).
- As garantias de semântica de negócio e validações (ex: `subtotal_amount > 0`).
- Os níveis de serviço (SLA/SLO), políticas de confidencialidade e governança.

---

## 4. Implementação Passo a Passo

A implementação da Sessão 06 foi executada em 5 etapas modulares e sequenciais:

### Passo 1: Construção dos Arquivos de Esquema Avro (`.avsc`)
Foram criados 6 arquivos JSON de esquema na pasta [ingestion/schemas/avro/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/). Cada esquema foi especificado com namespace (ex: `com.marketplace.order`), documentação dos campos e declaração estrita de tipos nulos utilizando unions `["null", "string"]` com `"default": null`.

### Passo 2: Construção do Gerenciador `SchemaRegistryManager`
No módulo [ingestion/schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schema_registry.py), foi implementada a classe `SchemaRegistryManager`. A classe gerencia chamadas HTTP para o Schema Registry utilizando a biblioteca `requests`, implementando:
- Método `register_schema(subject, schema)` para envio de novos esquemas.
- Método `get_latest_schema(subject)` e `get_schema_by_id(schema_id)`.
- Método `check_compatibility(subject, schema)` para validação prévia.
- Resiliência contra oscilações de rede via `_request_with_retry()` com *backoff* exponencial.

### Passo 3: Construção do Módulo de Serialização `AvroSerializerDeserializer`
No arquivo [ingestion/serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/serialization.py), desenvolveu-se a engine de serialização/deserialização:
- `validate()`: Utiliza a biblioteca `fastavro.validate` para testar a conformidade do dicionário Python contra o esquema antes da conversão binária.
- `serialize()`: Converte o dicionário em bytes usando `fastavro.schemaless_writer` e anexa o cabeçalho de 5 bytes via `struct.pack(">I", schema_id)`.
- `deserialize()`: Extrai os primeiros 5 bytes, valida o *magic byte* `0x00`, lê o *schema_id* via `struct.unpack(">I")`, recupera o esquema correspondente (via cache local ou Schema Registry) e decodifica a mensagem com `fastavro.schemaless_reader`.

### Passo 4: Automação do Script de Registro CLI (`register_schemas.py`)
Desenvolveu-se o script executável [scripts/register_schemas.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/register_schemas.py) para varrer o diretório de esquemas, carregar as variáveis de ambiente `.env` e registrar programaticamente todos os 6 esquemas no Confluent Schema Registry. O script utiliza um formatador de logs estruturados em formato JSON (`JsonFormatter`).

### Passo 5: Criação da Suíte de Testes com Pytest
Foram criadas 18 suítes de testes unitários e de integração em [tests/test_schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_schema_registry.py) e [tests/test_serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_serialization.py) utilizando a biblioteca `unittest.mock` para cenários off-line e suporte a testes de integração reais em containers Docker locais.

---

## 5. Análise de Código e Trechos Chave

### 5.1 Definição do Schema Avro (`order_creation.avsc`)
O trecho abaixo exemplifica a modelagem de registros aninhados (`ShippingAddress`), coleções de itens (`OrderItem`) e campos opcionais com `Unions` em Avro.

```json
// Trecho extraído de ingestion/schemas/avro/order_creation.avsc
{
  "type": "record",
  "name": "OrderCreationEvent",
  "namespace": "com.marketplace.order",
  "doc": "Esquema para o evento de criação de pedidos no checkout.",
  "fields": [
    { "name": "event_id", "type": "string", "doc": "ID do evento (UUID v4)" },
    { "name": "order_id", "type": "string", "doc": "ID do pedido (UUID v4)" },
    { "name": "total_amount", "type": "double", "doc": "Valor total pago" },
    {
      "name": "shipping_address",
      "type": {
        "type": "record",
        "name": "ShippingAddress",
        "fields": [
          { "name": "street", "type": "string" },
          { "name": "city", "type": "string" },
          {
            "name": "complement",
            "type": ["null", "string"],
            "default": null
          }
        ]
      }
    },
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "OrderItem",
          "fields": [
            { "name": "product_id", "type": "string" },
            { "name": "quantity", "type": "int" },
            { "name": "unit_price", "type": "double" }
          ]
        }
      }
    }
  ]
}
```

### 5.2 Empacotamento Binário do Confluent Wire Format (`serialization.py`)
No arquivo [ingestion/serialization.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/serialization.py#L121-L130), a codificação do cabeçalho de 5 bytes é realizada utilizando a biblioteca nativa `struct` do Python:

```python
# Trecho de serialização com o cabeçalho Confluent Wire Format (ingestion/serialization.py)
buffer = BytesIO()
if include_wire_header:
    if schema_id is None:
        raise SerializationError("schema_id é obrigatório para incluir o cabeçalho Wire Format.")
    
    # Byte 0: \x00 (Magic Byte) | Bytes 1-4: ID do Esquema em Big-Endian (Format String '>I')
    header = WIRE_FORMAT_MAGIC_BYTE + struct.pack(">I", schema_id)
    buffer.write(header)

# Escreve o payload binário schemaless via fastavro
fastavro.schemaless_writer(buffer, parsed_schema, payload)
return buffer.getvalue()
```

### 5.3 Deserialização e Leitura do Cabeçalho (`serialization.py`)
A função de deserialização lê e valida os 5 bytes iniciais antes de processar o corpo da mensagem ([ingestion/serialization.py#L159-L177](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/serialization.py#L159-L177)):

```python
# Trecho de extração do cabeçalho Confluent Wire Format
if has_wire_header:
    if len(data) < WIRE_FORMAT_HEADER_SIZE:
        raise DeserializationError("Mensagem inválida: tamanho menor que 5 bytes.")
    
    magic_byte = buffer.read(1)
    if magic_byte != WIRE_FORMAT_MAGIC_BYTE:
        raise DeserializationError(f"Magic Byte inválido: esperado b'\\x00', recebido {magic_byte!r}.")
    
    # Desempacota o ID de 32 bits a partir do formato Big-Endian
    schema_id = struct.unpack(">I", buffer.read(4))[0]

    # Resolve o esquema dinamicamente no Schema Registry se não foi fornecido
    if reader_schema is None and self.schema_registry_manager is not None:
        reader_schema = self.schema_registry_manager.get_schema_by_id(schema_id)
```

### 5.4 Formatação Estruturada de Logs em JSON (`register_schemas.py`)
Para auditar o processo de registro automatizado no CI/CD ou ambiente operacional, o script [scripts/register_schemas.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/register_schemas.py#L21-L38) estende a classe `logging.Formatter`:

```python
class JsonFormatter(logging.Formatter):
    """Formatador customizado para emissão de logs em formato JSON estruturado."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if hasattr(record, "subject"):
            log_data["subject"] = record.subject
        if hasattr(record, "schema_id"):
            log_data["schema_id"] = record.schema_id
        return json.dumps(log_data)
```

---

## 6. Desafios Enfrentados e Soluções

### Desafio 1: Latência na Consulta ao Schema Registry Durante a Deserialização
* **Problema:** Em um ambiente de streaming de alto throughput, fazer uma requisição HTTP REST ao Schema Registry a cada mensagem recebida para buscar o esquema pelo ID inviabilizaria o desempenho da ingestão PySpark.
* **Solução:** Implementou-se um mecanismo de **Cache de Esquemas em Memória** (`self._parsed_schemas_cache`) dentro de `AvroSerializerDeserializer`. Uma vez recuperado e compilado pelo `fastavro.parse_schema()`, o esquema fica armazenado em memória, reduzindo a latência de deserialização para milissegundos.

### Desafio 2: Divergência entre Formato Avro Schemaless e Confluent Wire Format
* **Problema:** Ferramentas utilitárias de leitura de arquivos `.avro` esperam dados em formato container Avro com cabeçalho contendo o JSON do esquema. O Kafka, por outro lado, utiliza a convenção da Confluent de 5 bytes seguidos por dados *schemaless*.
* **Solução:** O módulo `AvroSerializerDeserializer` foi desenhado com o parâmetro booleano `include_wire_header: bool = True`. Isso permite utilizar o mesmo módulo tanto para produzir mensagens no Kafka (com Wire Format) quanto para serializações brutas sem o cabeçalho (para testes unitários ou armazenamento direto em arquivos de bloco).

### Desafio 3: Isolamento de Testes Unitários Sem Dependência de Containers Ativos
* **Problema:** A execução da suíte de testes `pytest` no pipeline de CI/CD não deve falhar caso o container Docker do Confluent Schema Registry esteja desativado.
* **Solução:** Implementou-se um padrão duplo de testes em [tests/test_schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/test_schema_registry.py):
  1. Testes unitários puros isolados com objetos mockados via `unittest.mock.MagicMock`.
  2. Testes de integração sinalizados com `@pytest.mark.skipif`, que verificam a abertura da porta TCP `localhost:8081` através de um helper de *socket* (`is_service_available`), executando a verificação real apenas se a infraestrutura estiver no ar.

---

## 7. Trade-offs de Engenharia & Alternativas Consideradas

### 7.1 JSON Schema vs Apache Avro

| Critério | JSON Schema | Apache Avro (Escolha do Projeto) |
| :--- | :--- | :--- |
| **Tamanho do Payload Binário** | Grande (as chaves do JSON são repetidas em cada mensagem). | **Muito Compacto** (apenas os valores binários são enviados). |
| **Velocidade de Deserialização** | Moderada (parsing de texto JSON exige alocação frequente de strings). | **Extremamente Alta** (deserialização binária direta com `fastavro` em C/Python). |
| **Legibilidade Humana** | Direta (payloads legíveis sem ferramentas especiais). | Requer decodificador com esquema (resolvido por ferramentas como Schema Registry). |
| **Suporte ao Ecossistema Big Data** | Limitado em engines como Apache Spark/Flink. | **Nativo & First-Class** em Apache Spark (`from_avro`, `to_avro`), Delta Lake e Kafka Connect. |

*Decisão:* **Apache Avro** foi escolhido como o padrão exclusivo para o canal de eventos por sua alta performance de transporte binário e integração nativa perfeita com o ecossistema de Data Lakes.

### 7.2 Confluent Schema Registry + Avro vs Protocol Buffers (Protobuf) / gRPC

| Critério | Protobuf / gRPC | Confluent Schema Registry + Avro |
| :--- | :--- | :--- |
| **Foco Principal** | Comunicação RPC ponto a ponto síncrona entre microserviços. | **Governança de Event Streaming Assíncrono** sobre Apache Kafka. |
| **Histórico de Esquemas** | Compilado nos binários das aplicações via arquivos `.proto`. | **Gerenciado Dinamicamente** em tempo de execução via REST API no Schema Registry. |
| **Evolução de Schemas** | Baseada em numeração de tags (fields tags). | Baseada em regras de compatibilidade explícitas (`BACKWARD`, `FORWARD`, `FULL`). |

*Decisão:* Embora o Protobuf seja excelente para chamadas internas RPC entre microsserviços, o ecossistema **Confluent Schema Registry + Avro** é a referência de mercado para ecossistemas de dados orientados a streaming no Apache Kafka.

---

## 8. Boas Práticas e Padrões Recomendados

1. **Adote Regras de Compatibilidade BACKWARD por Padrão:** Garantir que novas versões de esquemas sejam *Backward Compatible* permite que consumidores legados (como pipelines PySpark rodando versões anteriores) continuem lendo dados produzidos por sistemas novos sem falhas.
2. **Defina Valores Padrão em Unions (`"default": null`):** Sempre que um campo puder assumir valores nulos, posicione o `"null"` no topo do array de tipos Avro `["null", "string"]` e atribua o valor `"default": null`. Isso evita exceções de validação durante alterações futuras no contrato.
3. **Impeça Chamadas HTTP Síncronas na Ingestão:** Nunca consulte o Schema Registry de maneira síncrona dentro da função interna de um consumidor de streaming. Utilize caches em memória para mapear `Schema ID -> Parsed Schema`.
4. **Utilize Logs Estruturados em JSON em Scripts de Governança:** Conforme demonstrado no [scripts/register_schemas.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/register_schemas.py), a emissão de logs em formato JSON facilita a parsing automática por sistemas de observabilidade como Grafana Loki, Datadog ou ELK Stack.

---

## 9. Perspectiva da Indústria e Casos Reais

- **Uber (Apache Kafka + Schema Service):** A Uber processa trilhões de mensagens por dia em seu cluster Kafka. O uso de serialização baseada em Avro com um repositório central de esquemas reduziu em mais de 60% o tráfego de rede da empresa em comparação com o formato JSON original.
- **Mercado Livre & Fintechs:** Plataformas de e-commerce e pagamentos usam o Confluent Schema Registry para impedir a presença de *"Poison Pills"* (mensagens malformatadas que travam tópicos de alta prioridade ou interrompem processos de liquidação financeira). Se um evento falhar na validação do schema, ele é rejeitado antes de entrar no Kafka ou direcionado para um tópico de *Dead Letter Queue (DLQ)*.

---

## 10. Exercícios Práticos e Próximos Passos

### Exercícios Sugeridos para Consolidação do Conhecimento

1. **Simulação de Alteração Incompatível de Esquema:**
   - Modifique o arquivo [order_creation.avsc](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schemas/avro/order_creation.avsc) removendo o campo `order_id` ou alterando o tipo de `total_amount` de `double` para `int`.
   - Execute a função `check_compatibility()` do [ingestion/schema_registry.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/schema_registry.py) e verifique se a incompatibilidade é capturada corretamente.

2. **Inspeção de Payload Binário:**
   - Utilize a classe `AvroSerializerDeserializer` para serializar o payload de um evento `cart_events`.
   - Imprima o conteúdo em formato hexadecimal (`encoded_bytes.hex()`) e identifique os 5 primeiros bytes (procurando pelo *magic byte* `00` seguido do Schema ID em formato hex).

3. **Validação de Testes Unitários Locais:**
   - Execute a suíte completa de testes no terminal através do ambiente virtual configurado no projeto:
     ```bash
     .venv/bin/pytest tests/test_schema_registry.py tests/test_serialization.py -v
     ```

### Próximos Passos no Roadmap

Com a conclusão do **Step 2 (Modelagem de Domínio de Negócio e Contratos de Dados)**, o projeto está pronto para avançar para o **Step 3 (Ingestão em Tempo Real & Event Streaming)**:
- Subir os containers do Apache Kafka, Zookeeper e Kafka Connect no ambiente Docker.
- Implementar os produtores (*Producers*) de teste publicando eventos reais codificados em Avro nos tópicos `marketplace.*`.
- Construir os pipelines de ingestão PySpark Streaming para leitura dos eventos e persistência bruta na Camada Bronze da arquitetura Medalhão.

