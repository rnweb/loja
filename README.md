# Loja Yampi — Automação de Catálogo (Bling-Safe) & Tema Rocket (`rnweb/loja`)

Repositório central para evolução do código do **Tema Rocket** (`Twig`, `Vue.js`, `SASS`) e atualização automatizada em lote de produtos na **API REST v2 da Yampi** preservando 100% da integração com o **ERP Bling / Dropi**.

## Estrutura do Repositório

```text
loja/
├── .github/workflows/
│   └── yampi-catalog-sync.yml          # GitHub Actions para buscar, enriquecer e atualizar produtos em lote
├── data/
│   ├── staging/                        # Manifesto JSON do catálogo (gerado pelo comando fetch/transform)
│   └── backups/                        # Snapshots automáticos de rollback antes de cada envio PUT
├── scripts/
│   └── yampi_batch_updater.py          # Pipeline Python modular com Rate Limiter (27 req/min) e proteção Bling
├── theme-rocket/                       # Código-fonte espelhado do Editor de Código da Yampi
│   ├── assets/css/
│   │   └── custom-product-specs.scss   # Estilos SASS para ficha técnica e descrição HTML
│   ├── components/
│   │   └── product-technical-specs.vue # Componente Vue.js de abas (Descrição, Ficha Técnica, Medidas)
│   ├── sections/
│   │   └── product-details-enriched.twig
│   └── templates/
│       └── product.twig
├── .env.example
├── .gitignore
└── README.md
```

## Configuração no GitHub (`https://github.com/rnweb/loja`)

1. Em **Settings → Secrets and variables → Actions**, adicione os 3 segredos da sua loja Yampi:
   - `YAMPI_ALIAS`: Alias da loja (presente na URL do painel/API).
   - `YAMPI_USER_TOKEN`: Token do usuário no painel da Yampi.
   - `YAMPI_USER_SECRET_KEY`: Chave secreta do usuário na Yampi.

## Como Usar Localmente ou com o Gemini

```bash
# 1. Rodar autoteste offline de integridade Bling-Safe
python3 scripts/yampi_batch_updater.py self-test

# 2. Baixar todos os produtos atuais da Yampi para data/staging/yampi_catalog_staging.json
python3 scripts/yampi_batch_updater.py --staging-file data/staging/yampi_catalog_staging.json fetch

# 3. Gerar/enriquecer descrição HTML, especificações técnicas, medidas e SEO em lote
python3 scripts/yampi_batch_updater.py --staging-file data/staging/yampi_catalog_staging.json transform --store-name "Minha Loja"

# 4. Validar payload em modo Dry-Run (sem alterar nada na loja)
python3 scripts/yampi_batch_updater.py --staging-file data/staging/yampi_catalog_staging.json push

# 5. Enviar alterações aprovadas para a Yampi com backup automático de rollback
python3 scripts/yampi_batch_updater.py --staging-file data/staging/yampi_catalog_staging.json push --execute
```

## Referências Oficiais Yampi

- [Introdução à API REST v2 da Yampi](https://docs.yampi.com.br/api-reference/introduction)
- [Endpoint de Listagem e Atualização de Produtos](https://docs.yampi.com.br/api-reference/catalogo/produtos/listar-produtos)
- [Limites de Requisição (Rate Limit de 30 req/min)](https://docs.yampi.com.br/faq/introduction)
- [Arquitetura do Editor de Código & Tema Rocket](https://docs.yampi.com.br/editor-codigo/intro)
- [Integração Bling na Yampi](https://help.yampi.com.br/pt-BR/articles/8377062-como-configurar-o-bling-na-yampi)
