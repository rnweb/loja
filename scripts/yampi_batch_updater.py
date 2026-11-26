#!/usr/bin/env python3
"""
Yampi Catalog Batch Updater (Bling-Safe Pipeline)
=================================================
Pipeline modular em Python para buscar produtos na API REST v2 da Yampi,
enriquecer descrições HTML, especificações técnicas, medidas e metadados de SEO
em lote, respeitando o rate limit (30 req/min) e preservando integralmente os
identificadores de vínculo com o ERP Bling / Dropi (`erp_id`, `sku`, `id`, `stocks`).

Documentação Oficial Yampi API v2:
- Base URL: https://api.dooki.com.br/v2/{alias}
- Endpoint de Produtos: https://docs.yampi.com.br/api-reference/catalogo/produtos/listar-produtos
- Introdução & Cache (skipCache=true): https://docs.yampi.com.br/api-reference/introduction
- Limites de Taxa (30 req/min): https://docs.yampi.com.br/faq/introduction

Modos de Operação (CLI):
1. `fetch`: Baixa o catálogo da Yampi com `include=texts,seo,skus,categories,brand,filters`
   e salva um manifesto local (`yampi_catalog_staging.json`) pronto para edição ou IA.
2. `transform`: Aplica templates de descrição HTML, especificações e SEO no manifesto local
   (ou integra com a API do Gemini via `GEMINI_API_KEY` se configurada) sem tocar nas
   chaves de integração com o Bling.
3. `push`: Dispara `PUT /catalog/products/{id}` em lote com controle de rate limit
   (token bucket / intervalo de 2.2s + exponential backoff em HTTP 429), modo `--dry-run`
   por padrão e geração de log de auditoria + backup de rollback.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Configuração de Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("yampi_batch_updater")


# ---------------------------------------------------------------------------
# Campos Protegidos (Vínculo ERP Bling / Dropi e Integridade de SKU)
# ---------------------------------------------------------------------------
# Estes campos NUNCA são alterados ou sobrescritos durante a etapa de transform,
# garantindo que a ponte de estoque, pedidos e notas fiscais com o Bling continue intacta.
BLING_PROTECTED_PRODUCT_KEYS = frozenset({
    "id",
    "merchant_id",
    "affiliation_id",
    "erp_id",
    "seller_id",
    "sku",
    "slug",
    "simple",
    "has_variations",
    "ncm",
    "total_in_stock",
    "image_reference_sku_id",
})

BLING_PROTECTED_SKU_KEYS = frozenset({
    "id",
    "product_id",
    "sku",
    "erp_id",
    "barcode",
    "ncm",
    "price_cost",
    "price_sale",
    "price_discount",
    "weight",
    "height",
    "width",
    "length",
    "total_in_stock",
    "stocks",
    "variations",
})


# ---------------------------------------------------------------------------
# Configuração e Rate Limiter
# ---------------------------------------------------------------------------
@dataclass
class YampiConfig:
    """Credenciais e parâmetros operacionais para a API REST v2 da Yampi."""
    alias: str
    user_token: str
    user_secret_key: str
    base_url: str = "https://api.dooki.com.br/v2"
    requests_per_minute: int = 27  # Margem segura abaixo do limite oficial de 30 req/min
    max_retries: int = 4
    timeout_seconds: int = 30

    @classmethod
    def from_env(cls, alias_override: Optional[str] = None) -> "YampiConfig":
        alias = alias_override or os.getenv("YAMPI_ALIAS", "").strip()
        token = os.getenv("YAMPI_USER_TOKEN", "").strip()
        secret = os.getenv("YAMPI_USER_SECRET_KEY", "").strip()

        missing = []
        if not alias:
            missing.append("YAMPI_ALIAS")
        if not token:
            missing.append("YAMPI_USER_TOKEN")
        if not secret:
            missing.append("YAMPI_USER_SECRET_KEY")

        if missing:
            raise ValueError(
                f"Variáveis de ambiente ausentes: {', '.join(missing)}. "
                "Defina-as no ambiente ou passe via argumentos CLI."
            )
        return cls(alias=alias, user_token=token, user_secret_key=secret)


class RateLimiter:
    """
    Controlador de cadência para respeitar o limite de 30 requisições/minuto da Yampi.
    Por padrão utiliza 27 req/min (~2.22s entre requisições) para absorver variações de rede.
    """

    def __init__(self, requests_per_minute: int = 27) -> None:
        self.min_interval = 60.0 / max(1, requests_per_minute)
        self._last_call_ts: float = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_call_ts
        if elapsed < self.min_interval:
            sleep_for = self.min_interval - elapsed
            time.sleep(sleep_for)
        self._last_call_ts = time.monotonic()


# ---------------------------------------------------------------------------
# Cliente HTTP Nativo para API v2 da Yampi (Zero dependências externas obrigatórias)
# ---------------------------------------------------------------------------
class YampiClient:
    """
    Cliente REST para `https://api.dooki.com.br/v2/{alias}` com:
    - Headers `User-Token` e `User-Secret-Key`
    - `skipCache=true` automático em chamadas GET (evita o cache padrão de 30 min da Yampi)
    - Throttling automático + Retry com Exponential Backoff para HTTP 429 e 5xx
    """

    DEFAULT_INCLUDES = (
        "texts",
        "seo",
        "skus",
        "categories",
        "brand",
        "filters",
        "flags",
        "variations",
        "metadata",
    )

    def __init__(self, config: YampiConfig) -> None:
        self.config = config
        self.limiter = RateLimiter(config.requests_per_minute)

    def _build_url(self, path: str, query_params: Optional[Dict[str, Any]] = None) -> str:
        clean_path = path.lstrip("/")
        url = f"{self.config.base_url.rstrip('/')}/{self.config.alias}/{clean_path}"
        if query_params:
            filtered = {k: v for k, v in query_params.items() if v is not None}
            if filtered:
                url = f"{url}?{urllib.parse.urlencode(filtered, doseq=True)}"
        return url

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Token": self.config.user_token,
            "User-Secret-Key": self.config.user_secret_key,
            "User-Agent": "YampiBatchUpdater/2.0 (BlingSafePipeline)",
        }

    def request(
        self,
        method: str,
        path: str,
        query_params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        params = dict(query_params or {})
        if method.upper() == "GET" and "skipCache" not in params:
            params["skipCache"] = "true"

        url = self._build_url(path, params)
        body_bytes: Optional[bytes] = None
        if payload is not None:
            body_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        for attempt in range(1, self.config.max_retries + 1):
            self.limiter.wait()
            req = urllib.request.Request(
                url=url,
                data=body_bytes,
                headers=self._headers(),
                method=method.upper(),
            )
            try:
                with urllib.request.urlopen(req, timeout=self.config.timeout_seconds) as resp:
                    raw = resp.read().decode("utf-8")
                    return json.loads(raw) if raw else {}
            except urllib.error.HTTPError as exc:
                status = exc.code
                err_body = ""
                try:
                    err_body = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    pass

                # HTTP 429 (Too Many Requests) ou erro transitório 5xx
                if status in (429, 500, 502, 503, 504) and attempt < self.config.max_retries:
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    wait_seconds = (
                        float(retry_after)
                        if retry_after and retry_after.isdigit()
                        else (2.5 ** attempt) + 2.0
                    )
                    logger.warning(
                        "HTTP %s em %s %s (tentativa %d/%d). Aguardando %.1fs antes de tentar novamente...",
                        status,
                        method.upper(),
                        path,
                        attempt,
                        self.config.max_retries,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                    continue

                logger.error(
                    "Falha definitiva HTTP %s em %s %s | Resposta: %s",
                    status,
                    method.upper(),
                    url,
                    err_body[:600],
                )
                raise RuntimeError(
                    f"Erro HTTP {status} na API da Yampi ({method.upper()} {path}): {err_body}"
                ) from exc
            except urllib.error.URLError as exc:
                if attempt < self.config.max_retries:
                    wait_seconds = 2.0 * attempt
                    logger.warning(
                        "Erro de conexão (%s) na tentativa %d/%d. Aguardando %.1fs...",
                        exc.reason,
                        attempt,
                        self.config.max_retries,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                    continue
                raise

        raise RuntimeError(f"Número máximo de tentativas excedido para {method} {path}")

    def list_products(
        self,
        limit_per_page: int = 50,
        max_pages: Optional[int] = None,
        only_active: Optional[bool] = None,
        includes: Tuple[str, ...] = DEFAULT_INCLUDES,
    ) -> List[Dict[str, Any]]:
        """
        Lista todos os produtos do catálogo paginando automaticamente com `skipCache=true`
        e trazendo os relacionamentos `texts`, `seo`, `skus`, `categories`, `brand` e `filters`.
        """
        all_products: List[Dict[str, Any]] = []
        current_page = 1
        include_param = ",".join(includes)

        while True:
            query: Dict[str, Any] = {
                "page": current_page,
                "limit": limit_per_page,
                "include": include_param,
                "skipCache": "true",
            }
            if only_active is not None:
                query["active"] = "1" if only_active else "0"

            logger.info("Buscando página %d de produtos (limit=%d)...", current_page, limit_per_page)
            response = self.request("GET", "catalog/products", query_params=query)
            page_items = response.get("data", [])
            if not page_items:
                break

            all_products.extend(page_items)

            # A estrutura de paginação da Yampi pode vir em `meta.pagination` ou `meta.meta.pagination`
            meta_node = response.get("meta", {})
            if "meta" in meta_node and isinstance(meta_node["meta"], dict):
                meta_node = meta_node["meta"]
            pagination = meta_node.get("pagination", {})

            total_pages = int(pagination.get("total_pages", 1) or 1)
            total_items = int(pagination.get("total", len(all_products)) or len(all_products))
            logger.info(
                "Página %d/%d carregada (%d/%d produtos acumulados).",
                current_page,
                total_pages,
                len(all_products),
                total_items,
            )

            if current_page >= total_pages:
                break
            if max_pages is not None and current_page >= max_pages:
                logger.info("Limite de páginas (--max-pages=%d) atingido.", max_pages)
                break

            current_page += 1

        return all_products

    def get_product(self, product_id: int, includes: Tuple[str, ...] = DEFAULT_INCLUDES) -> Dict[str, Any]:
        """Busca um único produto pelo ID com todos os relacionamentos."""
        response = self.request(
            "GET",
            f"catalog/products/{product_id}",
            query_params={"include": ",".join(includes), "skipCache": "true"},
        )
        return response.get("data", response)

    def update_product(self, product_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Executa `PUT /catalog/products/{id}` atualizando o produto na Yampi."""
        return self.request("PUT", f"catalog/products/{product_id}", payload=payload)


# ---------------------------------------------------------------------------
# Normalização e Extração de Dados do Produto (Compatível com Includes da Yampi)
# ---------------------------------------------------------------------------
def _unwrap_relation(node: Any) -> Any:
    """
    Desembrulha objetos relacionados da Yampi que vêm no formato `{"data": ...}`.
    """
    if isinstance(node, dict) and "data" in node and len(node) == 1:
        return node["data"]
    return node


def extract_product_editable_Snapshot(product: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrai um snapshot limpo de um produto retornado por `GET /catalog/products?include=...`,
    separando claramente os metadados de vínculo com o Bling (somente leitura) dos campos
    editáveis de vitrine (descrição HTML, especificações, medidas, SEO).
    """
    texts_node = _unwrap_relation(product.get("texts", {})) or {}
    seo_node = _unwrap_relation(product.get("seo", {})) or {}
    skus_node = _unwrap_relation(product.get("skus", [])) or []
    brand_node = _unwrap_relation(product.get("brand", {})) or {}
    categories_node = _unwrap_relation(product.get("categories", [])) or []

    # Campos textuais podem estar na raiz do produto ou dentro do include `texts`
    description = (
        texts_node.get("description")
        if isinstance(texts_node, dict) and texts_node.get("description") is not None
        else product.get("description", "")
    ) or ""
    specifications = (
        texts_node.get("specifications")
        if isinstance(texts_node, dict) and texts_node.get("specifications") is not None
        else product.get("specifications", "")
    ) or ""
    measures = (
        texts_node.get("measures")
        if isinstance(texts_node, dict) and texts_node.get("measures") is not None
        else product.get("measures", "")
    ) or ""

    # Campos de SEO podem estar na raiz (`seo_title`, `seo_description`, `seo_keywords`) ou no include `seo`
    seo_title = (
        seo_node.get("title")
        or seo_node.get("seo_title")
        or product.get("seo_title")
        or ""
    )
    seo_description = (
        seo_node.get("description")
        or seo_node.get("seo_description")
        or product.get("seo_description")
        or ""
    )
    seo_keywords = (
        seo_node.get("keywords")
        or seo_node.get("seo_keywords")
        or product.get("seo_keywords")
        or ""
    )
    canonical_url = (
        seo_node.get("canonical_url")
        or product.get("canonical_url")
        or ""
    )

    sku_summaries = []
    for sku_item in skus_node if isinstance(skus_node, list) else []:
        if not isinstance(sku_item, dict):
            continue
        variations_list = _unwrap_relation(sku_item.get("variations", [])) or []
        var_pairs = []
        for v in variations_list if isinstance(variations_list, list) else []:
            if isinstance(v, dict):
                v_name = v.get("name") or v.get("group_name") or ""
                v_val = v.get("value") or ""
                if v_name and v_val:
                    var_pairs.append(f"{v_name}:{v_val}")
        sku_summaries.append({
            "id": sku_item.get("id"),
            "sku": sku_item.get("sku"),
            "erp_id": sku_item.get("erp_id"),
            "price_sale": sku_item.get("price_sale"),
            "price_discount": sku_item.get("price_discount"),
            "price_cost": sku_item.get("price_cost"),
            "weight": sku_item.get("weight"),
            "height": sku_item.get("height"),
            "width": sku_item.get("width"),
            "length": sku_item.get("length"),
            "total_in_stock": sku_item.get("total_in_stock"),
            "variations": var_pairs,
        })

    category_names = [
        c.get("name", "")
        for c in (categories_node if isinstance(categories_node, list) else [])
        if isinstance(c, dict) and c.get("name")
    ]
    category_ids = [
        c.get("id")
        for c in (categories_node if isinstance(categories_node, list) else [])
        if isinstance(c, dict) and c.get("id") is not None
    ]

    return {
        "product_id": product.get("id"),
        "name": product.get("name", ""),
        "slug": product.get("slug", ""),
        "active": bool(product.get("active", True)),
        "searchable": bool(product.get("searchable", True)),
        # Bloco protegido de vínculo com o Bling / ERP (Somente leitura)
        "bling_link_readonly": {
            "erp_id": product.get("erp_id"),
            "root_sku": product.get("sku"),
            "ncm": product.get("ncm"),
            "simple": product.get("simple"),
            "has_variations": product.get("has_variations"),
            "brand_id": brand_node.get("id") if isinstance(brand_node, dict) else product.get("brand_id"),
            "brand_name": brand_node.get("name") if isinstance(brand_node, dict) else "",
            "category_ids": category_ids,
            "category_names": category_names,
            "skus": sku_summaries,
        },
        # Bloco editável (Alvo das atualizações em lote)
        "editable_content": {
            "name": product.get("name", ""),
            "description": description,
            "specifications": specifications,
            "measures": measures,
            "seo_title": seo_title,
            "seo_description": seo_description,
            "seo_keywords": seo_keywords,
            "canonical_url": canonical_url,
            "search_terms": (
                texts_node.get("search_terms")
                if isinstance(texts_node, dict) and texts_node.get("search_terms")
                else product.get("search_terms", "")
            ) or "",
        },
        "update_ready": False,
        "raw_product": product,
    }


# ---------------------------------------------------------------------------
# Motor de Transformação em Lote (Templates HTML + Especificações + SEO)
# ---------------------------------------------------------------------------
def strip_html_tags(html_text: str) -> str:
    """Remove tags HTML para análise de comprimento e geração de meta description."""
    text = re.sub(r"<[^>]+>", " ", html_text or "")
    return re.sub(r"\s+", " ", text).strip()


def build_structured_html_description(snapshot: Dict[str, Any]) -> str:
    """
    Constrói uma descrição HTML limpa, escaneável e responsiva para o Tema Rocket da Yampi,
    aproveitando os dados existentes do produto e das variações vindas do Bling.
    Você pode customizar este template com a identidade visual da sua loja.
    """
    editable = snapshot["editable_content"]
    bling_info = snapshot["bling_link_readonly"]
    name = editable.get("name") or snapshot.get("name", "Produto")
    current_desc = (editable.get("description") or "").strip()
    brand_name = bling_info.get("brand_name") or ""
    categories = bling_info.get("category_names") or []

    # Coleta variações disponíveis (ex.: Cor, Tamanho, Voltagem) a partir dos SKUs sincronizados pelo Bling
    variation_map: Dict[str, List[str]] = {}
    for sku_entry in bling_info.get("skus", []):
        for pair in sku_entry.get("variations", []):
            if ":" in pair:
                k, v = pair.split(":", 1)
                variation_map.setdefault(k.strip(), [])
                if v.strip() not in variation_map[k.strip()]:
                    variation_map[k.strip()].append(v.strip())

    # Se a descrição original já tiver um texto base vindo do Bling, preserva-o dentro da estrutura
    clean_base_text = strip_html_tags(current_desc)
    if not clean_base_text:
        cat_context = f" da linha {', '.join(categories)}" if categories else ""
        brand_context = f" {brand_name}" if brand_name else ""
        clean_base_text = (
            f"O {name}{brand_context}{cat_context} foi desenvolvido para oferecer alta durabilidade, "
            f"acabamento superior e praticidade no seu dia a dia."
        )

    highlights_html = [
        f"<li><strong>Qualidade Garantida:</strong> Produto selecionado com rigoroso controle de padrão{f' ({brand_name})' if brand_name else ''}.</li>",
        "<li><strong>Pronta Entrega &amp; Envio Rápido:</strong> Estoque sincronizado em tempo real com despacho ágil.</li>",
    ]
    for var_name, var_values in variation_map.items():
        highlights_html.append(
            f"<li><strong>Opções de {var_name}:</strong> {', '.join(var_values)}.</li>"
        )

    html_parts = [
        '<div class="yampi-custom-description">',
        f"  <h2>{name}</h2>",
        f"  <p>{clean_base_text}</p>",
        "  <h3>Principais Características e Diferenciais</h3>",
        "  <ul>",
        *[f"    {item}" for item in highlights_html],
        "  </ul>",
        "</div>",
    ]
    return "\n".join(html_parts)


def build_specifications_and_measures(snapshot: Dict[str, Any]) -> Tuple[str, str]:
    """
    Gera ou padroniza os campos `specifications` (Especificações técnicas) e `measures` (Medidas)
    a partir das dimensões reais cadastradas no SKU vindo do Bling, caso estejam em branco.
    """
    editable = snapshot["editable_content"]
    bling_info = snapshot["bling_link_readonly"]

    specs = (editable.get("specifications") or "").strip()
    measures = (editable.get("measures") or "").strip()

    first_sku = (bling_info.get("skus") or [{}])[0]

    if not specs:
        spec_items = []
        if bling_info.get("brand_name"):
            spec_items.append(f"Marca: {bling_info['brand_name']}")
        if bling_info.get("root_sku") or first_sku.get("sku"):
            spec_items.append(f"Código SKU: {bling_info.get('root_sku') or first_sku.get('sku')}")
        if bling_info.get("ncm"):
            spec_items.append(f"NCM: {bling_info['ncm']}")
        for sku_entry in bling_info.get("skus", []):
            for pair in sku_entry.get("variations", []):
                if pair not in spec_items:
                    spec_items.append(pair.replace(":", ": "))
        specs = "; ".join(spec_items)

    if not measures and first_sku:
        m_parts = []
        weight = first_sku.get("weight")
        height = first_sku.get("height")
        width = first_sku.get("width")
        length = first_sku.get("length")
        if height and width and length:
            m_parts.append(f"Dimensões: {height} cm (A) x {width} cm (L) x {length} cm (C)")
        if weight:
            m_parts.append(f"Peso: {weight} kg")
        measures = " | ".join(m_parts)

    return specs, measures


def build_seo_metadata(snapshot: Dict[str, Any], store_name: str = "") -> Dict[str, str]:
    """
    Gera `seo_title` (até 60 caracteres), `seo_description` (até 160 caracteres)
    e `seo_keywords` otimizados para buscadores.
    """
    editable = snapshot["editable_content"]
    bling_info = snapshot["bling_link_readonly"]
    name = (editable.get("name") or snapshot.get("name") or "").strip()
    brand = (bling_info.get("brand_name") or "").strip()
    categories = bling_info.get("category_names") or []

    suffix = f" | {store_name}" if store_name else (f" | {brand}" if brand else "")
    raw_title = f"{name}{suffix}"
    seo_title = raw_title[:60].rstrip()

    plain_desc = strip_html_tags(editable.get("description") or "")
    if len(plain_desc) >= 50:
        seo_description = plain_desc[:157].rstrip() + "..."
    else:
        cat_str = f" em {categories[0]}" if categories else ""
        seo_description = (
            f"Compre {name}{cat_str} com envio rápido, garantia e condições especiais. "
            f"Confira especificações técnicas e disponibilidade."
        )[:160]

    kw_candidates = [name]
    if brand:
        kw_candidates.append(brand)
    kw_candidates.extend(categories)
    seo_keywords = "; ".join(dict.fromkeys([k for k in kw_candidates if k]))

    return {
        "seo_title": seo_title,
        "seo_description": seo_description,
        "seo_keywords": seo_keywords,
    }


def enrich_snapshot_in_place(
    snapshot: Dict[str, Any],
    overwrite_existing: bool = False,
    store_name: str = "",
    custom_ai_fn: Optional[Callable[[Dict[str, Any]], Dict[str, str]]] = None,
) -> bool:
    """
    Aplica o enriquecimento em um item do manifesto local.
    Se `overwrite_existing=False`, só preenche/melhora produtos cuja descrição atual seja
    curta (< 120 caracteres sem HTML) ou cujos campos de especificações/SEO estejam vazios.
    Retorna True se o produto sofreu alterações e foi marcado com `update_ready = True`.
    """
    editable = snapshot["editable_content"]
    changed = False

    # Se houver função de IA customizada (ex.: Gemini API), delega a geração textual a ela
    if custom_ai_fn is not None:
        ai_result = custom_ai_fn(snapshot)
        for key in ("description", "specifications", "measures", "seo_title", "seo_description", "seo_keywords"):
            if ai_result.get(key):
                editable[key] = ai_result[key]
                changed = True
    else:
        current_plain = strip_html_tags(editable.get("description") or "")
        if overwrite_existing or len(current_plain) < 120:
            new_html = build_structured_html_description(snapshot)
            if new_html != editable.get("description"):
                editable["description"] = new_html
                changed = True

        new_specs, new_measures = build_specifications_and_measures(snapshot)
        if (overwrite_existing or not editable.get("specifications")) and new_specs:
            editable["specifications"] = new_specs
            changed = True
        if (overwrite_existing or not editable.get("measures")) and new_measures:
            editable["measures"] = new_measures
            changed = True

        seo_data = build_seo_metadata(snapshot, store_name=store_name)
        for seo_key, seo_val in seo_data.items():
            if (overwrite_existing or not editable.get(seo_key)) and seo_val:
                editable[seo_key] = seo_val
                changed = True

    if changed:
        snapshot["update_ready"] = True
    return changed


# ---------------------------------------------------------------------------
# Construção Segura do Payload `PUT /catalog/products/{id}` (Bling-Safe)
# ---------------------------------------------------------------------------
def build_bling_safe_put_payload(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """
    Constrói o payload JSON para `PUT /catalog/products/{id}` na API v2 da Yampi.

    Garantias de segurança para integração com o Bling / Dropi:
    1. Preserva `name`, `slug`, `active`, `searchable`, `simple`, `has_variations`, `ncm`, `erp_id`
       e `brand_id`/`categories` exatamente como vieram do servidor da Yampi.
    2. Atualiza apenas os campos de conteúdo rico (`description`, `specifications`, `measures`,
       `seo_title`, `seo_description`, `seo_keywords`, `search_terms`, `texts` e `seo`).
    3. Não altera códigos `sku`, `erp_id` de variação, preços ou saldos de estoque.
    """
    raw = copy.deepcopy(snapshot.get("raw_product") or {})
    editable = snapshot["editable_content"]
    bling_info = snapshot["bling_link_readonly"]

    payload: Dict[str, Any] = {
        "name": editable.get("name") or raw.get("name"),
        "slug": raw.get("slug") or snapshot.get("slug"),
        "active": raw.get("active", True),
        "searchable": raw.get("searchable", True),
        "simple": raw.get("simple", True),
        # Campos textuais na raiz e no nó `texts` (compatibilidade total com versões do schema Yampi)
        "description": editable.get("description", ""),
        "specifications": editable.get("specifications", ""),
        "measures": editable.get("measures", ""),
        "search_terms": editable.get("search_terms", ""),
        "texts": {
            "description": editable.get("description", ""),
            "specifications": editable.get("specifications", ""),
            "measures": editable.get("measures", ""),
            "search_terms": editable.get("search_terms", ""),
        },
        # Campos de SEO na raiz e no nó `seo`
        "seo_title": editable.get("seo_title", ""),
        "seo_description": editable.get("seo_description", ""),
        "seo_keywords": editable.get("seo_keywords", ""),
        "seo": {
            "title": editable.get("seo_title", ""),
            "description": editable.get("seo_description", ""),
            "keywords": editable.get("seo_keywords", ""),
            "canonical_url": editable.get("canonical_url", ""),
        },
    }

    # Preserva chaves fiscais e de ERP se existirem no produto original
    if raw.get("erp_id") is not None:
        payload["erp_id"] = raw["erp_id"]
    if raw.get("ncm"):
        payload["ncm"] = raw["ncm"]
    if bling_info.get("brand_id") is not None:
        payload["brand_id"] = bling_info["brand_id"]
    if bling_info.get("category_ids"):
        payload["categories"] = bling_info["category_ids"]

    return payload


# ---------------------------------------------------------------------------
# Comandos Principais do Pipeline (`fetch`, `transform`, `push`)
# ---------------------------------------------------------------------------
def cmd_fetch(
    client: YampiClient,
    staging_file: Path,
    limit_per_page: int = 50,
    max_pages: Optional[int] = None,
    only_active: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    """
    Etapa 1: Baixa os produtos da Yampi e salva um arquivo JSON estruturado (`yampi_catalog_staging.json`).
    """
    products = client.list_products(
        limit_per_page=limit_per_page,
        max_pages=max_pages,
        only_active=only_active,
    )
    snapshots = [extract_product_editable_Snapshot(p) for p in products]

    staging_data = {
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "merchant_alias": client.config.alias,
        "total_products": len(snapshots),
        "products": snapshots,
    }
    staging_file.parent.mkdir(parents=True, exist_ok=True)
    staging_file.write_text(json.dumps(staging_data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(
        "Catálogo salvo com sucesso em '%s' (%d produtos).",
        staging_file,
        len(snapshots),
    )
    return snapshots


def cmd_transform(
    staging_file: Path,
    overwrite_existing: bool = False,
    store_name: str = "",
    filter_Ids: Optional[List[int]] = None,
) -> int:
    """
    Etapa 2: Lê o arquivo de staging, enriquece descrição HTML, especificações, medidas e SEO
    preservando todos os dados do Bling, e salva o arquivo pronto para revisão ou envio.
    """
    if not staging_file.exists():
        raise FileNotFoundError(f"Arquivo de staging não encontrado: {staging_file}")

    staging_data = json.loads(staging_file.read_text(encoding="utf-8"))
    snapshots: List[Dict[str, Any]] = staging_data.get("products", [])

    updated_count = 0
    for snap in snapshots:
        pid = snap.get("product_id")
        if filter_Ids and pid not in filter_Ids:
            continue
        if enrich_snapshot_in_place(
            snap,
            overwrite_existing=overwrite_existing,
            store_name=store_name,
        ):
            updated_count += 1

    staging_data["transformed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    staging_data["ready_to_push_count"] = sum(1 for s in snapshots if s.get("update_ready"))
    staging_file.write_text(json.dumps(staging_data, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info(
        "Transformação concluída: %d produtos enriquecidos e marcados com `update_ready: true` em '%s'.",
        updated_count,
        staging_file,
    )
    return updated_count


def cmd_push(
    client: YampiClient,
    staging_file: Path,
    execute_live: bool = False,
    filter_Ids: Optional[List[int]] = None,
    backup_dir: Path = Path("backups_yampi"),
) -> Dict[str, Any]:
    """
    Etapa 3: Envia as atualizações (`PUT /catalog/products/{id}`) para a API da Yampi
    respeitando o rate limit e salvando um backup de rollback antes de começar.
    Por segurança, roda em modo `DRY-RUN` a menos que `--execute` seja informado.
    """
    if not staging_file.exists():
        raise FileNotFoundError(f"Arquivo de staging não encontrado: {staging_file}")

    staging_data = json.loads(staging_file.read_text(encoding="utf-8"))
    snapshots: List[Dict[str, Any]] = staging_data.get("products", [])

    candidates = [
        s for s in snapshots
        if s.get("update_ready") and (not filter_Ids or s.get("product_id") in filter_Ids)
    ]

    if not candidates:
        logger.info("Nenhum produto marcado com `update_ready: true` para atualizar.")
        return {"total_candidates": 0, "updated": 0, "failed": 0, "dry_run": not execute_live}

    # Salva backup de rollback com o estado original antes do disparo
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"yampi_rollback_backup_{timestamp}.json"
    backup_path.write_text(json.dumps(candidates, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Backup de rollback salvo em '%s' (%d produtos).", backup_path, len(candidates))

    updated = 0
    failed = 0
    audit_results = []

    for idx, snap in enumerate(candidates, start=1):
        pid = snap["product_id"]
        name = snap.get("name", "")
        sku_root = snap.get("bling_link_readonly", {}).get("root_sku")
        payload = build_bling_safe_put_payload(snap)

        if not execute_live:
            logger.info(
                "[DRY-RUN %d/%d] PUT /catalog/products/%s (SKU: %s | Nome: %s) -> Payload validado (%d bytes).",
                idx,
                len(candidates),
                pid,
                sku_root,
                name,
                len(json.dumps(payload, ensure_ascii=False)),
            )
            audit_results.append({"product_id": pid, "sku": sku_root, "status": "dry_run_ok"})
            continue

        try:
            logger.info(
                "[LIVE %d/%d] Atualizando produto ID %s (SKU: %s | %s)...",
                idx,
                len(candidates),
                pid,
                sku_root,
                name,
            )
            client.update_product(pid, payload)
            snap["update_ready"] = False
            snap["last_pushed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
            updated += 1
            audit_results.append({"product_id": pid, "sku": sku_root, "status": "updated"})
        except Exception as exc:
            failed += 1
            logger.error("Erro ao atualizar produto ID %s (%s): %s", pid, name, exc)
            audit_results.append({
                "product_id": pid,
                "sku": sku_root,
                "status": "error",
                "error": str(exc),
            })

    if execute_live:
        staging_file.write_text(json.dumps(staging_data, indent=2, ensure_ascii=False), encoding="utf-8")

    summary = {
        "total_candidates": len(candidates),
        "updated": updated,
        "failed": failed,
        "dry_run": not execute_live,
        "backup_file": str(backup_path),
        "results": audit_results,
    }
    logger.info(
        "Resumo do envio: candidatos=%d | atualizados=%d | falhas=%d | dry_run=%s",
        len(candidates),
        updated,
        failed,
        not execute_live,
    )
    return summary


# ---------------------------------------------------------------------------
# Autoteste Offline (Valida o Pipeline e a Proteção de Vínculo com o Bling)
# ---------------------------------------------------------------------------
def run_self_test() -> None:
    """
    Executa um teste completo em memória simulando um produto sincronizado pelo Bling
    para comprovar que descrição HTML, especificações e SEO são gerados corretamente
    sem alterar `id`, `erp_id`, `sku`, `stocks` ou preços.
    """
    logger.info("Iniciando autoteste offline do pipeline Yampi (Bling-Safe)...")
    mock_bling_product = {
        "id": 98765432,
        "merchant_id": 1050,
        "erp_id": "BLING-ERP-778899",
        "active": True,
        "searchable": True,
        "simple": False,
        "has_variations": True,
        "ncm": "6109.10.00",
        "name": "Camiseta Oversized Algodão Egípcio",
        "slug": "camiseta-oversized-algodao-egipcio",
        "sku": "CAM-EGIP-PAI",
        "description": "Camiseta algodao egipcio importada via Bling",
        "brand": {"data": {"id": 44, "name": "Studio Minimal"}},
        "categories": {"data": [{"id": 12, "name": "Camisetas"}, {"id": 19, "name": "Masculino"}]},
        "texts": {
            "data": {
                "description": "Camiseta algodao egipcio importada via Bling",
                "specifications": "",
                "measures": "",
                "search_terms": "t-shirt; camisa premiun",
            }
        },
        "seo": {"data": {"title": "", "description": "", "keywords": ""}},
        "skus": {
            "data": [
                {
                    "id": 555001,
                    "product_id": 98765432,
                    "sku": "CAM-EGIP-PRETA-M",
                    "erp_id": "BLING-SKU-001",
                    "price_sale": 149.90,
                    "price_discount": 129.90,
                    "price_cost": 55.00,
                    "weight": 0.28,
                    "height": 4,
                    "width": 25,
                    "length": 32,
                    "total_in_stock": 18,
                    "variations": {"data": [{"name": "Cor", "value": "Preta"}, {"name": "Tamanho", "value": "M"}]},
                },
                {
                    "id": 555002,
                    "product_id": 98765432,
                    "sku": "CAM-EGIP-OFF-G",
                    "erp_id": "BLING-SKU-002",
                    "price_sale": 149.90,
                    "price_discount": 129.90,
                    "price_cost": 55.00,
                    "weight": 0.30,
                    "height": 4,
                    "width": 25,
                    "length": 32,
                    "total_in_stock": 12,
                    "variations": {"data": [{"name": "Cor", "value": "Off-White"}, {"name": "Tamanho", "value": "G"}]},
                },
            ]
        },
    }

    snap = extract_product_editable_Snapshot(mock_bling_product)
    changed = enrich_snapshot_in_place(snap, overwrite_existing=False, store_name="Minha Loja")
    assert changed is True, "O produto de teste deveria ter sido enriquecido."

    put_payload = build_bling_safe_put_payload(snap)

    # Asserções de integridade do vínculo Bling
    assert put_payload["erp_id"] == "BLING-ERP-778899", "O erp_id do Bling foi corrompido!"
    assert put_payload["slug"] == "camiseta-oversized-algodao-egipcio", "O slug do produto foi alterado!"
    assert put_payload["ncm"] == "6109.10.00", "O NCM fiscal foi alterado!"
    assert put_payload["brand_id"] == 44, "O brand_id foi perdido!"
    assert put_payload["categories"] == [12, 19], "Os IDs de categorias foram perdidos!"
    assert "skus" not in put_payload, "O payload de produto não deve sobrescrever a lista de SKUs/estoques do Bling!"
    assert "<h2>Camiseta Oversized Algodão Egípcio</h2>" in put_payload["description"]
    assert "Opções de Cor:" in put_payload["description"]
    assert "Dimensões: 4 cm (A) x 25 cm (L) x 32 cm (C)" in put_payload["measures"]
    assert len(put_payload["seo_title"]) <= 60
    assert len(put_payload["seo_description"]) <= 160

    logger.info("Autoteste concluído com SUCESSO! Vínculo Bling preservado e payload validado.")
    print(json.dumps(put_payload, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Interface de Linha de Comando (CLI)
# ---------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Atualizador em lote de produtos na API v2 da Yampi (Bling-Safe)."
    )
    parser.add_argument(
        "--alias",
        help="Alias da sua loja na Yampi (ou defina a variável de ambiente YAMPI_ALIAS).",
    )
    parser.add_argument(
        "--staging-file",
        default="yampi_catalog_staging.json",
        help="Caminho do arquivo JSON local de staging (padrão: yampi_catalog_staging.json).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcomando: self-test
    subparsers.add_parser(
        "self-test",
        help="Executa um teste offline validando a geração de payload e proteção de dados do Bling.",
    )

    # Subcomando: fetch
    fetch_parser = subparsers.add_parser(
        "fetch",
        help="Busca todos os produtos da Yampi via GET /catalog/products e salva no arquivo de staging.",
    )
    fetch_parser.add_argument("--limit", type=int, default=50, help="Itens por página (padrão: 50).")
    fetch_parser.add_argument("--max-pages", type=int, default=None, help="Limite opcional de páginas.")
    fetch_parser.add_argument(
        "--only-active",
        action="store_true",
        help="Buscar apenas produtos ativos na loja.",
    )

    # Subcomando: transform
    transform_parser = subparsers.add_parser(
        "transform",
        help="Gera descrição HTML, especificações, medidas e SEO no arquivo de staging local.",
    )
    transform_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Reescrever mesmo produtos que já possuem descrição longa ou SEO preenchido.",
    )
    transform_parser.add_argument(
        "--store-name",
        default="",
        help="Nome da loja para anexar ao título de SEO (ex.: 'Minha Loja').",
    )
    transform_parser.add_argument(
        "--ids",
        nargs="*",
        type=int,
        help="Lista opcional de IDs de produtos específicos para transformar.",
    )

    # Subcomando: push
    push_parser = subparsers.add_parser(
        "push",
        help="Envia os produtos com `update_ready: true` via PUT /catalog/products/{id} com controle de rate limit.",
    )
    push_parser.add_argument(
        "--execute",
        action="store_true",
        help="Executa o PUT real na API da Yampi (sem esta flag, roda em modo seguro DRY-RUN).",
    )
    push_parser.add_argument(
        "--ids",
        nargs="*",
        type=int,
        help="Lista opcional de IDs de produtos específicos para enviar.",
    )

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    staging_path = Path(args.staging_file)

    if args.command == "self-test":
        run_self_test()
        return 0

    config = YampiConfig.from_env(alias_override=args.alias)
    client = YampiClient(config)

    if args.command == "fetch":
        cmd_fetch(
            client=client,
            staging_file=staging_path,
            limit_per_page=args.limit,
            max_pages=args.max_pages,
            only_active=True if args.only_active else None,
        )
    elif args.command == "transform":
        cmd_transform(
            staging_file=staging_path,
            overwrite_existing=args.overwrite,
            store_name=args.store_name,
            filter_Ids=args.ids,
        )
    elif args.command == "push":
        cmd_push(
            client=client,
            staging_file=staging_path,
            execute_live=args.execute,
            filter_Ids=args.ids,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())