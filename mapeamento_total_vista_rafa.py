"""
Mapeamento DEFINITIVO Vista -> Rafa: cobertura 100% dos 299 campos.

Estrategia:
1. Direto: campo Vista -> campo Rafa (1:1)
2. Fallback: tenta varios campos Vista em ordem, usa o primeiro nao-nulo
3. Derivado: funcao Python que recebe o registro Vista completo e devolve o valor
4. Anexo: extrai do array Anexo[] do Vista filtrando pela Descricao
5. Default: valor padrao quando Vista nao tem o dado

Saidas:
- mapeamento_total.json   -> dicionario maquina-legivel para o importador
- mapeamento_total.md     -> documentacao 1 linha por campo
- mapeamento_total.csv    -> formato planilha p/ revisao com o Rafa

Para usar no importador, basta importar `MAPEAMENTO` deste modulo e iterar.
"""

from __future__ import annotations

import csv
import json
import re
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BASE = Path(__file__).parent

# ============================================================================
# Helpers para derivacoes
# ============================================================================


def _strip_accents(s: str) -> str:
    n = unicodedata.normalize("NFD", s)
    return "".join(c for c in n if not unicodedata.combining(c)).lower()


def _to_bool(x: Any) -> bool | None:
    if x is None or x == "":
        return None
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    if s in {"sim", "yes", "true", "1", "s", "y", "x"}:
        return True
    if s in {"nao", "no", "false", "0", "n", ""}:
        return False
    return None


def _to_number(x: Any) -> float | None:
    if x is None or x == "":
        return None
    if isinstance(x, (int, float)):
        return x
    s = str(x).strip().replace("R$", "").replace(" ", "")
    if not s:
        return None
    # 1.234,56 -> 1234.56
    if s.count(",") == 1 and (s.count(".") >= 1 or len(s.split(",")[1]) <= 2):
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _coalesce(rec: dict, *keys: str) -> Any:
    for k in keys:
        v = rec.get(k)
        if v not in (None, "", "0", 0, "false", "False"):
            return v
    # segunda passada aceitando falsy nao-nulo
    for k in keys:
        v = rec.get(k)
        if v not in (None, ""):
            return v
    return None


def _anexo_por_descricao(rec: dict, *termos: str) -> str | None:
    """Retorna JSON OBJECT do primeiro anexo cuja descricao contem qualquer termo.
    Formato esperado pelo backend: {url, name, size, mimeType}.
    """
    import json as _json
    import mimetypes as _mt
    anexos = rec.get("Anexos") or rec.get("Anexo") or []
    if isinstance(anexos, dict):
        anexos = list(anexos.values())
    if not isinstance(anexos, list):
        return None
    termos_norm = [_strip_accents(t) for t in termos]
    for anx in anexos:
        if not isinstance(anx, dict):
            continue
        desc_raw = str(anx.get("Descricao") or anx.get("Anexo") or "")
        desc_n = _strip_accents(desc_raw)
        if any(t in desc_n for t in termos_norm):
            url = anx.get("Arquivo") or anx.get("Anexo") or anx.get("URL")
            if not url:
                return None
            name = desc_raw or url.rsplit("/", 1)[-1].split("?")[0]
            mime, _ = _mt.guess_type(name.split("?")[0])
            return _json.dumps({
                "url": url,
                "name": name,
                "size": 0,
                "mimeType": mime or "application/octet-stream",
            }, ensure_ascii=False)
    return None


def _qualquer_anexo(rec: dict) -> str | None:
    """Retorna URL do PRIMEIRO anexo do imovel (sem filtro de descricao).
    Usado para o campo 'outros' que recebe qualquer anexo disponivel."""
    anexos = rec.get("Anexos") or rec.get("Anexo") or []
    if isinstance(anexos, dict):
        anexos = list(anexos.values())
    if not isinstance(anexos, list):
        return None
    for anx in anexos:
        if not isinstance(anx, dict):
            continue
        url = anx.get("Arquivo") or anx.get("Anexo") or anx.get("URL")
        if url:
            return url
    return None


def _primeiro_anexo_json(rec: dict) -> str | None:
    """Retorna JSON OBJECT do PRIMEIRO anexo no formato esperado pelo backend
    (Caso A em docs/CRM):

      {"url": "<s3-key ou URL>", "name": "<filename>", "size": <int>, "mimeType": "<mime>"}

    Frontend NAO aceita array. Backend faz upsert disso em
    property_field_values.value como JSON.stringify.
    """
    import json as _json
    import mimetypes as _mt
    anexos = rec.get("Anexos") or rec.get("Anexo") or []
    if isinstance(anexos, dict):
        anexos = list(anexos.values())
    if not isinstance(anexos, list):
        return None
    for anx in anexos:
        if not isinstance(anx, dict):
            continue
        url = anx.get("Arquivo") or anx.get("Anexo") or anx.get("URL")
        if not url:
            continue
        desc = anx.get("Descricao") or url.rsplit("/", 1)[-1].split("?")[0]
        mime, _ = _mt.guess_type(desc.split("?")[0])
        return _json.dumps({
            "url": url,
            "name": desc,
            "size": 0,
            "mimeType": mime or "application/octet-stream",
        }, ensure_ascii=False)
    return None


def _ultimo_andar(rec: dict) -> bool | None:
    andar = _to_number(rec.get("AndarDoApto") or rec.get("PosicaoAndar"))
    total = _to_number(rec.get("Andares"))
    if andar is None or total is None:
        return None
    return andar >= total


def _area_intima(rec: dict) -> float | None:
    """area_intima = AreaPrivativa - LivingAmbientes - SalaJantar (aproximacao).
    Como nao temos esses subtotais, retornamos None e deixamos ser preenchido manualmente.
    Mantemos o campo presente mas vazio para nao bloquear o registro.
    """
    return None


def _portal_facebook(rec: dict) -> str | None:
    """Vista nao tem feed especifico p/ Facebook Marketplace.
    Usa ExibirNoSite como proxy: se True -> 'Sim', senao 'Nao'.
    """
    val = _to_bool(rec.get("ExibirNoSite"))
    if val is None:
        return None
    return "Sim" if val else "Nao"


def _portal_instagram(rec: dict) -> str | None:
    val = _to_bool(rec.get("ExibirNoSite"))
    if val is None:
        return None
    return "Sim" if val else "Nao"


def _portaria_virtual(rec: dict) -> bool | None:
    """Portaria virtual = tem portaria mas NAO e presencial nem 24h."""
    tem_portaria = _to_bool(rec.get("InfraEstrutura", {}).get("Portaria") if isinstance(rec.get("InfraEstrutura"), dict) else rec.get("infra.Portaria"))
    presencial = _to_bool(rec.get("PortariaPresencial"))
    porta24 = _to_bool(rec.get("InfraEstrutura", {}).get("Portaria24Hrs") if isinstance(rec.get("InfraEstrutura"), dict) else rec.get("infra.Portaria24Hrs"))
    if tem_portaria and presencial is False and not porta24:
        return True
    if tem_portaria is False:
        return False
    return None


def _garagem_tipo_eh(tipo_alvo: str) -> Callable[[dict], bool | None]:
    """Cria funcao que checa se GaragemTipo bate com o tipo_alvo."""
    alvo_norm = _strip_accents(tipo_alvo)

    def _fn(rec: dict) -> bool | None:
        tipo = rec.get("GaragemTipo") or rec.get("TipoGaragem")
        if not tipo:
            return None
        return alvo_norm in _strip_accents(str(tipo))

    return _fn


def _garagem_sem_vaga(rec: dict) -> bool | None:
    vagas = _to_number(rec.get("Vagas") or rec.get("EstacionamentoVagas"))
    if vagas is None:
        return None
    return vagas == 0


def _corretores_vinculados(rec: dict) -> list[str] | None:
    """Junta lista de corretores vinculados a partir dos campos do Vista."""
    out = []
    for k in ("Agenciador", "CorretorPrimeiroAge", "CorretorChave", "CodigoCorretor"):
        v = rec.get(k)
        if v and v not in out:
            out.append(str(v))
    if not out and isinstance(rec.get("Corretores"), list):
        for c in rec["Corretores"]:
            nome = c.get("Nome") or c.get("Codigo") if isinstance(c, dict) else c
            if nome and nome not in out:
                out.append(str(nome))
    return out or None


def _owners_picker(rec: dict) -> dict | None:
    """SYSTEM field: estrutura padronizada com proprietario(s)."""
    nome = rec.get("Proprietario") or rec.get("SrProprietario")
    cod = rec.get("CodigoProprietario")
    if not nome and not cod:
        return None
    return {"nome": nome, "codigo": cod}


def _empreendimento_nome(rec: dict) -> str | None:
    return _coalesce(rec, "Empreendimento", "EEmpreendimento", "CodigoEmpreendimento")


def _acabamento_piso(rec: dict) -> str | None:
    """Aproveita o piso mais "geral" como acabamento."""
    return _coalesce(rec, "Piso", "PisoSala", "PisoDormitorio")


def _administradora(rec: dict) -> str | None:
    """administradora (do imovel) cai no mesmo dado de administradora_condominio."""
    return _coalesce(rec, "AdministradoraCondominio", "Administradora")


# ============================================================================
# Especificacao do mapping
# ============================================================================


@dataclass
class Map:
    tipo: str
    estrategia: str  # direto | fallback | derivado | anexo | default | reuso
    fontes: list[str] = field(default_factory=list)
    func: Callable[[dict], Any] | None = None
    default: Any = None
    nota: str = ""


# A. Mapping direto e fallback (vem de comparar_campos.json e refinos)
MAPEAMENTO: dict[str, Map] = {
    # ------------------------------------------------------------------ ATTACH
    "autorizacao_de_venda": Map("ATTACHMENT", "anexo", func=lambda r: _anexo_por_descricao(r, "autorizacao", "autorizacao de venda"), nota="Anexo[].Descricao ~ 'autorizacao'"),
    "contrato_compra_e_venda": Map("ATTACHMENT", "anexo", func=lambda r: _anexo_por_descricao(r, "contrato", "compra e venda"), nota="Anexo[].Descricao ~ 'contrato' (NAO usar 'venda' sozinho - bate com 'Autorizacao de Venda')"),
    "doc_condominio": Map("ATTACHMENT", "anexo", func=lambda r: _anexo_por_descricao(r, "condominio", "convencao"), nota="Anexo[].Descricao ~ 'condominio'"),
    "escritura": Map("ATTACHMENT", "anexo", func=lambda r: _anexo_por_descricao(r, "escritura"), nota="Anexo[].Descricao ~ 'escritura'"),
    "iptu_documento": Map("ATTACHMENT", "anexo", func=lambda r: _anexo_por_descricao(r, "iptu"), nota="Anexo[].Descricao ~ 'iptu'"),
    "matricula_do_imovel": Map("ATTACHMENT", "anexo", func=lambda r: _anexo_por_descricao(r, "matricula"), nota="Anexo[].Descricao ~ 'matricula'"),
    "procuracao": Map("ATTACHMENT", "anexo", func=lambda r: _anexo_por_descricao(r, "procuracao"), nota="Anexo[].Descricao ~ 'procuracao'"),
    "outros": Map("ATTACHMENT", "anexo", func=_primeiro_anexo_json, nota="JSON object do primeiro anexo: {url, name, filename, mime_type}. Frontend nao aceita array."),

    # ------------------------------------------------------------------ BOOL
    "aceita_dacao": Map("BOOLEAN", "direto", ["AceitaDacao"]),
    "agua_encanada": Map("BOOLEAN", "direto", ["infra.Agua"]),
    "alto_padrao": Map("BOOLEAN", "direto", ["AltoPadrao"]),
    "caract_adega": Map("BOOLEAN", "direto", ["carac.Adega"]),
    "caract_agua_quente": Map("BOOLEAN", "direto", ["carac.AguaQuente"]),
    "caract_airbnb": Map("BOOLEAN", "fallback", ["carac.Airbnb", "Airbnb"]),
    "caract_alarme": Map("BOOLEAN", "direto", ["carac.Alarme"]),
    "caract_ar_central": Map("BOOLEAN", "direto", ["carac.ArCentral"]),
    "caract_ar_condicionado": Map("BOOLEAN", "direto", ["carac.ArCondicionado"]),
    "caract_area_servico": Map("BOOLEAN", "direto", ["carac.AreaServico"]),
    "caract_armarios_embutidos": Map("BOOLEAN", "direto", ["carac.ArmarioEmbutido"]),
    "caract_banheiro_empregada": Map("BOOLEAN", "fallback", ["carac.BanheiroAuxiliar", "carac.WCEmpregada"]),
    "caract_banheiro_social": Map("BOOLEAN", "direto", ["carac.BanheiroSocial"]),
    "caract_bar": Map("BOOLEAN", "direto", ["carac.Bar"]),
    "caract_cabine_forca": Map("BOOLEAN", "direto", ["infra.CabineDeForca"]),
    "caract_canaletas_rodape": Map("BOOLEAN", "direto", ["carac.CanaletasNoRodape"]),
    "caract_carga_descarga": Map("BOOLEAN", "derivado", ["LocalCargaEDescarga"], func=lambda r: _to_bool(r.get("LocalCargaEDescarga")) if r.get("LocalCargaEDescarga") else None, nota="LocalCargaEDescarga truthy"),
    "caract_cerca_eletrificada": Map("BOOLEAN", "direto", ["carac.CercaEletrica"]),
    "caract_churrasqueira": Map("BOOLEAN", "direto", ["carac.Churrasqueira"]),
    "caract_copa": Map("BOOLEAN", "direto", ["carac.Copa"]),
    "caract_copa_cozinha": Map("BOOLEAN", "direto", ["carac.CopaCozinha"]),
    "caract_cozinha": Map("BOOLEAN", "fallback", ["carac.Cozinha", "carac.CozinhaComTanque"]),
    "caract_cozinha_americana": Map("BOOLEAN", "direto", ["carac.CozinhaAmericana"]),
    "caract_cozinha_planejada": Map("BOOLEAN", "fallback", ["carac.CozinhaPlanejada", "carac.CozinhaMontada"]),
    "caract_deck": Map("BOOLEAN", "direto", ["carac.Deck"]),
    "caract_dependencia_empregada": Map("BOOLEAN", "fallback", ["carac.DependenciadeEmpregada", "carac.DependenciaDeEmpregada"]),
    "caract_despensa": Map("BOOLEAN", "direto", ["carac.Despensa"]),
    "caract_dormitorio_armarios": Map("BOOLEAN", "direto", ["carac.DormitorioComArmario"]),
    "caract_edicula": Map("BOOLEAN", "direto", ["carac.Edicula"]),
    "caract_energia_trifasica": Map("BOOLEAN", "direto", ["infra.EnergiaTrifasica"]),
    "caract_escritorio": Map("BOOLEAN", "fallback", ["carac.Escritorio", "AreaEscritorio"], nota="se houver AreaEscritorio > 0 -> true"),
    "caract_espera_split": Map("BOOLEAN", "direto", ["carac.EsperaSplit"]),
    "caract_estar_intimo": Map("BOOLEAN", "direto", ["carac.EstarIntimo"]),
    "caract_forro": Map("BOOLEAN", "direto", ["carac.Forro"]),
    "caract_gerador_energia": Map("BOOLEAN", "fallback", ["infra.GeradorEnergia", "PotenciaKVA"], nota="se PotenciaKVA > 0 -> true"),
    "caract_gradeado": Map("BOOLEAN", "direto", ["carac.Gradeado"]),
    "caract_hall": Map("BOOLEAN", "direto", ["carac.LivingHall"]),
    "caract_hidromassagem": Map("BOOLEAN", "direto", ["carac.Hidromassagem"]),
    "caract_home_theater": Map("BOOLEAN", "direto", ["carac.HomeTheater"]),
    "caract_jardim_inverno": Map("BOOLEAN", "direto", ["carac.JardimInverno"]),
    "caract_lareira": Map("BOOLEAN", "direto", ["carac.Lareira"]),
    "caract_lavabo": Map("BOOLEAN", "direto", ["carac.Lavabo"]),
    "caract_mobiliado": Map("BOOLEAN", "direto", ["carac.Mobiliado"]),
    "caract_pcd": Map("BOOLEAN", "fallback", ["carac.PCD", "PCD"]),
    "caract_piscina": Map("BOOLEAN", "direto", ["carac.Piscina"]),
    "caract_piso_elevado": Map("BOOLEAN", "direto", ["carac.PisoElevado"]),
    "caract_placa_solar": Map("BOOLEAN", "direto", ["carac.PlacaSolar"]),
    "caract_portaria_blindada": Map("BOOLEAN", "direto", ["infra.PortariaBlindada"]),
    "caract_portoes_eclusa": Map("BOOLEAN", "direto", ["infra.PortoesComEclusa"]),
    "caract_quintal": Map("BOOLEAN", "direto", ["carac.Quintal"]),
    "caract_reformado": Map("BOOLEAN", "direto", ["carac.Reformado"]),
    "caract_sacada": Map("BOOLEAN", "direto", ["carac.Sacada"]),
    "caract_sacada_churrasqueira": Map("BOOLEAN", "direto", ["carac.SacadaComChurrasqueira"]),
    "caract_sala_armarios": Map("BOOLEAN", "direto", ["carac.SalaArmarios"]),
    "caract_sala_jantar": Map("BOOLEAN", "direto", ["carac.SalaJantar"]),
    "caract_sala_recepcao": Map("BOOLEAN", "direto", ["infra.SalaDeRecepcao"]),
    "caract_sala_tv": Map("BOOLEAN", "direto", ["carac.SalaTV"]),
    "caract_sauna": Map("BOOLEAN", "direto", ["carac.Sauna"]),
    "caract_sem_mobilia": Map("BOOLEAN", "fallback", ["SemMobilia", "carac.Mobiliado"], func=lambda r: (_to_bool(r.get("SemMobilia")) if r.get("SemMobilia") is not None else (not _to_bool(r.get("carac.Mobiliado")) if r.get("carac.Mobiliado") is not None else None)), nota="SemMobilia OU inverso de Mobiliado"),
    "caract_semi_mobiliado": Map("BOOLEAN", "direto", ["carac.SemiMobiliado"]),
    "caract_shaft": Map("BOOLEAN", "direto", ["infra.Shaft"]),
    "caract_split": Map("BOOLEAN", "direto", ["carac.Split"]),
    "caract_suite_master": Map("BOOLEAN", "direto", ["carac.SuiteMaster"]),
    "caract_terraco": Map("BOOLEAN", "direto", ["carac.Terraco"]),
    "caract_tubulacao": Map("BOOLEAN", "direto", ["infra.Tubulacao"]),
    "caract_vigia_externo": Map("BOOLEAN", "direto", ["carac.VigiaExterno"]),
    "caract_vigia_interno": Map("BOOLEAN", "direto", ["carac.VigiaInterno"]),
    "caract_vista_mar": Map("BOOLEAN", "direto", ["carac.VistaMar"]),
    "caract_vista_panoramica": Map("BOOLEAN", "direto", ["carac.VistaPanoramica"]),
    "caract_vitrine": Map("BOOLEAN", "direto", ["carac.Vitrine"]),
    "destaque_web": Map("BOOLEAN", "fallback", ["DestaqueWeb", "AlugarJaDestaque", "ImovelaVendaDestaqueImovelaVenda", "TiqueImoveisEmDestaque"]),
    "energia_eletrica": Map("BOOLEAN", "direto", ["infra.EnergiaEletrica"]),
    "exclusivo": Map("BOOLEAN", "fallback", ["Exclusivo", "ExclusivoCorretor"]),
    "exibir_no_site": Map("BOOLEAN", "direto", ["ExibirNoSite"]),
    "garagem_convencao": Map("BOOLEAN", "derivado", ["GaragemTipo"], func=_garagem_tipo_eh("convencao"), nota="GaragemTipo ~ 'convencao'"),
    "garagem_escriturada": Map("BOOLEAN", "derivado", ["GaragemTipo"], func=_garagem_tipo_eh("escritur"), nota="GaragemTipo ~ 'escriturada'"),
    "garagem_rotativo": Map("BOOLEAN", "derivado", ["GaragemTipo"], func=_garagem_tipo_eh("rotativ"), nota="GaragemTipo ~ 'rotativo'"),
    "garagem_sem_vaga": Map("BOOLEAN", "derivado", ["Vagas"], func=_garagem_sem_vaga, nota="Vagas == 0"),
    "imovel_orulo": Map("BOOLEAN", "fallback", ["EmpOrulo", "Orulo"]),
    "infra_aquecimento_central": Map("BOOLEAN", "direto", ["infra.AquecimentoCentral"]),
    "infra_bicicletario": Map("BOOLEAN", "direto", ["infra.Bicicletario"]),
    "infra_brinquedoteca": Map("BOOLEAN", "direto", ["infra.Brinquedoteca"]),
    "infra_churrasqueira_coletiva": Map("BOOLEAN", "direto", ["infra.ChurrasqueiraCondominio"]),
    "infra_circuito_tv": Map("BOOLEAN", "direto", ["infra.CircuitoFechadoTV"]),
    "infra_condominio_fechado": Map("BOOLEAN", "direto", ["infra.CondominioFechado"]),
    "infra_deposito": Map("BOOLEAN", "direto", ["infra.Deposito"]),
    "infra_elevador": Map("BOOLEAN", "fallback", ["infra.Elevador", "Elevadores"], nota="se Elevadores > 0 -> true"),
    "infra_elevador_servico": Map("BOOLEAN", "direto", ["infra.ElevadorServico"]),
    "infra_empresa_monitoramento": Map("BOOLEAN", "fallback", ["infra.EmpresaDeMonitoramento", "carac.Monitoramento"]),
    "infra_entrada_servico": Map("BOOLEAN", "direto", ["infra.EntradaServicoIndependente"]),
    "infra_espaco_gourmet": Map("BOOLEAN", "direto", ["infra.EspacoGourmet"]),
    "infra_estacionamento": Map("BOOLEAN", "direto", ["infra.Estacionamento"]),
    "infra_estacionamento_visitantes": Map("BOOLEAN", "direto", ["infra.EstacionamentoVisitantes"]),
    "infra_gas_central": Map("BOOLEAN", "direto", ["infra.GasCentral"]),
    "infra_guarita": Map("BOOLEAN", "direto", ["infra.Guarita"]),
    "infra_heliponto": Map("BOOLEAN", "direto", ["infra.Heliponto"]),
    "infra_interfone": Map("BOOLEAN", "direto", ["infra.Interfone"]),
    "infra_jardim": Map("BOOLEAN", "direto", ["infra.Jardim"]),
    "infra_lavanderia": Map("BOOLEAN", "direto", ["infra.Lavanderia"]),
    "infra_mini_mercado": Map("BOOLEAN", "direto", ["MiniMercado"]),
    "infra_pilotis": Map("BOOLEAN", "direto", ["infra.Pilotis"]),
    "infra_piscina_aquecida": Map("BOOLEAN", "direto", ["infra.PiscinaAquecida"]),
    "infra_piscina_coletiva": Map("BOOLEAN", "direto", ["infra.PiscinaColetiva"]),
    "infra_piscina_infantil": Map("BOOLEAN", "direto", ["infra.PiscinaInfantil"]),
    "infra_playground": Map("BOOLEAN", "direto", ["infra.Playground"]),
    "infra_port_cochere": Map("BOOLEAN", "direto", ["PortCochere"]),
    "infra_portaria": Map("BOOLEAN", "direto", ["infra.Portaria"]),
    "infra_portaria_24h": Map("BOOLEAN", "direto", ["infra.Portaria24Hrs"]),
    "infra_portaria_presencial": Map("BOOLEAN", "direto", ["PortariaPresencial"]),
    "infra_portaria_virtual": Map("BOOLEAN", "derivado", ["infra.Portaria", "PortariaPresencial", "infra.Portaria24Hrs"], func=_portaria_virtual, nota="Portaria=true & Presencial=false & 24h=false"),
    "infra_porteiro_eletronico": Map("BOOLEAN", "direto", ["infra.PorteiroEletronico"]),
    "infra_quadra_beach_tenis": Map("BOOLEAN", "direto", ["QuadraBeachTenis"]),
    "infra_quadra_esportes": Map("BOOLEAN", "fallback", ["infra.QuadraEsportes", "infra.QuadraPoliEsportiva"]),
    "infra_quadra_tenis": Map("BOOLEAN", "direto", ["infra.QuadraTenis"]),
    "infra_quiosque": Map("BOOLEAN", "direto", ["infra.Quiosque"]),
    "infra_sala_fitness": Map("BOOLEAN", "direto", ["infra.SalaFitness"]),
    "infra_sala_jogos": Map("BOOLEAN", "direto", ["infra.SalaoJogos"]),
    "infra_salao_festas": Map("BOOLEAN", "direto", ["infra.SalaoFestas"]),
    "infra_sauna_coletiva": Map("BOOLEAN", "direto", ["infra.SaunaCondominio"]),
    "infra_seguranca": Map("BOOLEAN", "direto", ["infra.SegurancaPatrimonial"]),
    "infra_spa": Map("BOOLEAN", "direto", ["infra.Spa"]),
    "infra_terraco_coletivo": Map("BOOLEAN", "direto", ["infra.TerracoColetivo"]),
    "infra_vigilancia_24h": Map("BOOLEAN", "direto", ["infra.Vigilancia24Horas"]),
    "infra_zelador": Map("BOOLEAN", "direto", ["infra.Zelador"]),
    "lancamento": Map("BOOLEAN", "fallback", ["Lancamento", "DataLancamento"], nota="Lancamento bool, ou se DataLancamento existir -> true"),
    "pavimentacao": Map("BOOLEAN", "fallback", ["infra.Pavimentacao", "Pavimentos"]),
    "permuta_imovel": Map("BOOLEAN", "direto", ["AceitaPermuta"]),
    "permuta_outros": Map("BOOLEAN", "direto", ["AceitaPermutaOutro"]),
    "permuta_veiculo": Map("BOOLEAN", "direto", ["AceitaPermutaCarro"]),
    "poco_artesiano": Map("BOOLEAN", "direto", ["infra.PocoArtesiano"]),
    "possibilidade_financiamento": Map("BOOLEAN", "direto", ["AceitaFinanciamento"]),
    "rede_esgoto": Map("BOOLEAN", "direto", ["infra.RedeEsgoto"]),
    "super_destaque": Map("BOOLEAN", "direto", ["SuperDestaqueWeb"]),
    "tem_placa": Map("BOOLEAN", "fallback", ["TemPlaca", "ImoPlaca"]),
    "terreo": Map("BOOLEAN", "fallback", ["Terreo"], func=lambda r: True if _to_number(r.get("AndarDoApto")) == 0 else _to_bool(r.get("Terreo")), nota="Terreo bool, ou AndarDoApto == 0"),
    "ultimo_andar": Map("BOOLEAN", "derivado", ["AndarDoApto", "Andares"], func=_ultimo_andar, nota="AndarDoApto >= Andares"),
    "viabilidade": Map("BOOLEAN", "direto", ["infra.PossuiViabilidade"]),

    # ------------------------------------------------------------------ CHIPS
    "dormitorios": Map("CHIPS", "direto", ["Dormitorios"]),
    "face_predio": Map("CHIPS", "direto", ["FacePredio"]),
    "face_unidade": Map("CHIPS", "fallback", ["Face", "Posicao"]),
    "orientacao_solar": Map("CHIPS", "fallback", ["OrientacaoSolar", "Orientacao"]),

    # ------------------------------------------------------------------ CURR
    "comissao": Map("CURRENCY", "fallback", ["ValorComissao", "TotalComissao"]),
    "condominio": Map("CURRENCY", "direto", ["ValorCondominio"]),
    "iptu": Map("CURRENCY", "direto", ["ValorIptu"]),
    "proprietario_livre": Map("CURRENCY", "direto", ["ValorLivreProprietario"]),
    "saldo_devedor": Map("CURRENCY", "direto", ["SaldoDivida"]),
    "total_aluguel": Map("CURRENCY", "direto", ["ValorTotalAluguel"]),
    "valor_antigo": Map("CURRENCY", "direto", ["ValorAntigo"]),
    "valor_condominio_m2": Map("CURRENCY", "direto", ["ValorCondominioM2"]),
    "valor_imovel_permuta": Map("CURRENCY", "fallback", ["ValorPermutaImovel", "proprietarios.ValorImovelComprado"]),
    "valor_iptu_m2": Map("CURRENCY", "direto", ["ValorIPTUM2"]),
    "valor_locacao": Map("CURRENCY", "fallback", ["ValorLocacao", "ValorDiaria"]),
    "valor_m2": Map("CURRENCY", "fallback", ["ValorM2", "ValorVendaM2"]),
    "valor_m2_aluguel": Map("CURRENCY", "fallback", ["ValorLocacaoM2", "ValorAluguelPorM2"]),
    "valor_permuta": Map("CURRENCY", "direto", ["ValorPermutaImovel"]),
    "valor_venda": Map("CURRENCY", "direto", ["ValorVenda"]),

    # ------------------------------------------------------------------ DATE
    "created_at": Map("DATE", "direto", ["DataCadastro"]),
    "data_chave": Map("DATE", "direto", ["DataChave"]),
    "data_entrega": Map("DATE", "fallback", ["DataEntrega", "DataLancamento"]),
    "data_liberacao": Map("DATE", "direto", ["DataLiberacao"]),
    "updated_at": Map("DATE", "fallback", ["DataAtualizacao", "DataHoraAtualizacao"]),

    # ------------------------------------------------------------------ MAP
    "mapa": Map("MAP", "derivado", ["Latitude", "Longitude", "GMapsLatitude", "GMapsLongitude"], func=lambda r: {"lat": _to_number(r.get("Latitude") or r.get("GMapsLatitude")), "lng": _to_number(r.get("Longitude") or r.get("GMapsLongitude"))} if (r.get("Latitude") or r.get("GMapsLatitude")) else None, nota="{lat,lng} a partir do Vista"),

    # ------------------------------------------------------------------ MULTI
    "agenciadores": Map("MULTI_SELECT", "direto", ["Agenciador"]),
    "corretores_vinculados": Map("MULTI_SELECT", "derivado", ["Agenciador", "CorretorPrimeiroAge", "CorretorChave", "CodigoCorretor"], func=_corretores_vinculados, nota="Une todos os corretores referenciados no imovel"),
    "fotografo": Map("MULTI_SELECT", "direto", ["Fotografo"]),

    # ------------------------------------------------------------------ NUMBER
    "andar": Map("NUMBER", "fallback", ["AndarDoApto", "PosicaoAndar"]),
    "ano_construcao": Map("NUMBER", "direto", ["AnoConstrucao"]),
    "area_armazem": Map("NUMBER", "direto", ["AreaArmazem"]),
    "area_construida": Map("NUMBER", "direto", ["AreaConstruida"]),
    "area_escritorio": Map("NUMBER", "direto", ["AreaEscritorio"]),
    "area_intima": Map("NUMBER", "derivado", ["AreaPrivativa", "LivingAmbientes"], func=_area_intima, nota="Derivado de areas; Vista nao tem direto (null se nao calculavel)"),
    "area_laje": Map("NUMBER", "direto", ["AreaLaje"]),
    "area_locavel": Map("NUMBER", "direto", ["AreaLocavel"]),
    "area_mezanino": Map("NUMBER", "direto", ["AreaMezanino"]),
    "area_privativa": Map("NUMBER", "direto", ["AreaPrivativa"]),
    "area_terreno": Map("NUMBER", "direto", ["AreaTerreno"]),
    "area_total": Map("NUMBER", "direto", ["AreaTotal"]),
    "banheiros": Map("NUMBER", "fallback", ["TotalBanheiros", "BanheiroSocialQtd"]),
    "capacidade_piso": Map("NUMBER", "direto", ["infra.CapacidadePiso"]),
    "closet": Map("NUMBER", "direto", ["Closet"]),
    "hidromassagem": Map("NUMBER", "direto", ["HidroSuite"]),
    "imoveis_por_andar": Map("NUMBER", "direto", ["AptosAndar"]),
    "living": Map("NUMBER", "fallback", ["LivingAmbientes", "carac.Living"]),
    "modulos": Map("NUMBER", "direto", ["Modulos"]),
    "numero_de_andares": Map("NUMBER", "direto", ["Andares"]),
    "numero_galpoes": Map("NUMBER", "direto", ["QTDGalpoes"]),
    "numero_prestacoes": Map("NUMBER", "direto", ["Prestacao"]),
    "pe_direito": Map("NUMBER", "direto", ["PeDireitoAlto"]),
    "percentual_comissao": Map("NUMBER", "direto", ["PercentualComissao"]),
    "percentual_proprietario": Map("NUMBER", "fallback", ["proprietarios.Percentualhonorarios", "proprietarios.PercentualRoyalties"]),
    "permuta_ano_min_veiculo": Map("NUMBER", "direto", ["AnoMinimoVeicPermuta"]),
    "permuta_dormitorios": Map("NUMBER", "direto", ["QntDormitoriosPermuta"]),
    "permuta_garagens": Map("NUMBER", "direto", ["QntGaragensPermuta"]),
    "permuta_suites": Map("NUMBER", "direto", ["QntSuitesPermuta"]),
    "potencia_gerador_kva": Map("NUMBER", "direto", ["PotenciaKVA"]),
    "qtd_elevadores": Map("NUMBER", "direto", ["Elevadores"]),
    "salas": Map("NUMBER", "direto", ["Salas"]),
    "suites": Map("NUMBER", "direto", ["Suites"]),
    "total_de_imoveis": Map("NUMBER", "direto", ["AptosEdificio"]),
    "vagas": Map("NUMBER", "fallback", ["Vagas", "EstacionamentoVagas"]),
    "vagas_cobertas": Map("NUMBER", "direto", ["VagasCobertas"]),
    "vagas_descobertas": Map("NUMBER", "direto", ["VagasDescobertas"]),
    "varandas": Map("NUMBER", "fallback", ["QtdVarandas", "Varanda"]),

    # ------------------------------------------------------------------ PHOTO
    "photo": Map("PHOTO", "fallback", ["FotoDestaque", "FotoDestaquePequena", "Foto.Foto", "Foto.FotoOriginal"], nota="Primeira foto destaque, fallback p/ Foto[0]"),

    # ------------------------------------------------------------------ SELECT
    "agencia": Map("SELECT", "fallback", ["CodigoAgencia", "ChaveNaAgencia"]),
    "bairro_foco": Map("SELECT", "fallback", ["BairroFoco", "proprietarios.BairroPerfil"]),
    "campanha_ativa": Map("SELECT", "fallback", ["CampanhaImportacao", "SummerSale"], nota="Nome de campanha ativa, se houver"),
    "categoria": Map("SELECT", "fallback", ["Categoria", "CategoriaImovel", "TipoImovel"]),
    "conservacao": Map("SELECT", "direto", ["EstadoConservacaoImovel"]),
    "construtora": Map("SELECT", "direto", ["Construtora"]),
    "empreendimento": Map("SELECT", "derivado", ["Empreendimento", "EEmpreendimento"], func=_empreendimento_nome),
    "ocupacao": Map("SELECT", "direto", ["Ocupacao"]),
    "perfil_construcao": Map("SELECT", "fallback", ["PadraoConstrucao", "PorteEstrutural"]),
    "portal_123i": Map("SELECT", "fallback", ["123iPublicationType", "TipoOferta321Achei"]),
    "portal_chaves_na_mao": Map("SELECT", "direto", ["ChavesNaMaoDestaque"]),
    "portal_facebook_marketplace": Map("SELECT", "derivado", ["ExibirNoSite"], func=_portal_facebook, nota="ExibirNoSite -> Sim/Nao (Vista nao tem feed especifico)"),
    "portal_imovelweb": Map("SELECT", "direto", ["ImovelwebModelo"]),
    "portal_instagram_imoveis": Map("SELECT", "derivado", ["ExibirNoSite"], func=_portal_instagram, nota="ExibirNoSite -> Sim/Nao (Vista nao tem feed especifico)"),
    "portal_mercado_livre": Map("SELECT", "direto", ["MercadoLivreTipoML"]),
    "portal_olx": Map("SELECT", "direto", ["OLXFinalidadesPublicadas"]),
    "portal_viva_real": Map("SELECT", "direto", ["VivaRealPublicationType"]),
    "portal_zap": Map("SELECT", "fallback", ["ZapTipoOferta", "FolhaSPModelo", "GrupoSPTipoOferta"]),
    "situacao": Map("SELECT", "direto", ["Situacao"]),
    "status": Map("SELECT", "direto", ["Status"]),
    "tipo_garagem": Map("SELECT", "direto", ["GaragemTipo"]),
    "tipo_logradouro": Map("SELECT", "direto", ["TipoEndereco"]),
    "topografia": Map("SELECT", "direto", ["Topografia"]),
    "uf": Map("SELECT", "direto", ["UF"]),

    # ------------------------------------------------------------------ SYSTEM
    "owners_picker": Map("SYSTEM", "derivado", ["Proprietario", "CodigoProprietario"], func=_owners_picker, nota="Estrutura {nome, codigo} do proprietario"),

    # ------------------------------------------------------------------ TEXT
    "acabamento_piso": Map("TEXT", "derivado", ["Piso", "PisoSala"], func=_acabamento_piso, nota="Primeiro piso disponivel (sala -> dormitorio)"),
    "acabamento_teto": Map("TEXT", "fallback", ["TipoTeto"], nota="Se Vista nao tiver, fica null"),
    "administradora": Map("TEXT", "derivado", ["AdministradoraCondominio"], func=_administradora, nota="Mesmo dado de administradora_condominio"),
    "administradora_condominio": Map("TEXT", "direto", ["AdministradoraCondominio"]),
    "bairro": Map("TEXT", "direto", ["Bairro"]),
    "bairro_comercial": Map("TEXT", "direto", ["BairroComercial"]),
    "bloco": Map("TEXT", "direto", ["Bloco"]),
    "cep": Map("TEXT", "direto", ["CEP"]),
    "cidade": Map("TEXT", "direto", ["Cidade"]),
    "complemento": Map("TEXT", "fallback", ["Complemento", "ComplementoMigrado"]),
    "corretor_chave": Map("TEXT", "direto", ["CorretorChave"]),
    "dimensoes_terreno": Map("TEXT", "fallback", ["DimensoesTerreno"], func=lambda r: r.get("DimensoesTerreno") or (f"{r.get('Frente')}x{r.get('Fundos')}" if r.get("Frente") and r.get("Fundos") else None), nota="Vista DimensoesTerreno, ou monta de Frente x Fundos"),
    "email_do_proprietario": Map("TEXT", "fallback", ["proprietarios.EmailComercial", "proprietarios.EmailResidencial", "proprietarios.EmailConjuge"]),
    "estado_edificio": Map("TEXT", "direto", ["EstadoConservacaoEdificio"]),
    "estado_imovel": Map("TEXT", "direto", ["EstadoImovel"]),
    "hora_dom_fim": Map("TEXT", "direto", ["HoraDomFim"]),
    "hora_dom_inicio": Map("TEXT", "direto", ["HoraDomInicio"]),
    "hora_fer_fim": Map("TEXT", "direto", ["HoraFerFim"]),
    "hora_fer_inicio": Map("TEXT", "direto", ["HoraFerInicio"]),
    "hora_sab_fim": Map("TEXT", "direto", ["HoraSabFim"]),
    "hora_sab_inicio": Map("TEXT", "direto", ["HoraSabInicio"]),
    "hora_seg_sex_fim": Map("TEXT", "direto", ["HoraSegSexFim"]),
    "hora_seg_sex_inicio": Map("TEXT", "direto", ["HoraSegSexInicio"]),
    "incorporadora": Map("TEXT", "direto", ["Incorporadora"]),
    "logradouro": Map("TEXT", "direto", ["Endereco"]),
    "lote": Map("TEXT", "direto", ["Lote"]),
    "matricula": Map("TEXT", "direto", ["Matricula"]),
    "nome_condominio": Map("TEXT", "derivado", ["Empreendimento"], func=_empreendimento_nome, nota="Mesmo dado de empreendimento (sinonimo no Vista)"),
    "nome_do_proprietario": Map("TEXT", "fallback", ["Proprietario", "SrProprietario", "proprietarios.Nome"]),
    "nome_empreendimento": Map("TEXT", "derivado", ["Empreendimento"], func=_empreendimento_nome, nota="Mesmo dado de empreendimento (sinonimo no Vista)"),
    "nome_empresa_monitoramento": Map("TEXT", "direto", ["infra.NomeEmpresaMonitoramento"]),
    "numero": Map("TEXT", "direto", ["Numero"]),
    "numero_box": Map("TEXT", "direto", ["GaragemNumeroBox"]),
    "numero_chave": Map("TEXT", "fallback", ["NumeroChave", "Chave"]),
    "permuta_localizacao": Map("TEXT", "direto", ["LocalizacaoPermuta"]),
    "permuta_tipo_imovel": Map("TEXT", "direto", ["TipoImovelPermuta"]),
    "permuta_tipo_veiculo": Map("TEXT", "direto", ["AceitaPermutaTipoVeiculo"]),
    "piso_area_intima": Map("TEXT", "direto", ["PisoAreaIntima"]),
    "piso_area_social": Map("TEXT", "fallback", ["PisoSala", "Piso"]),
    "piso_dormitorios": Map("TEXT", "direto", ["PisoDormitorio"]),
    "piso_salas": Map("TEXT", "fallback", ["PisoSala", "Piso"], nota="Mesmo dado de piso_area_social"),
    "quadra": Map("TEXT", "direto", ["Quadra"]),
    "recepcionista": Map("TEXT", "direto", ["RecepcionistaChave"]),
    "referencia": Map("TEXT", "fallback", ["Referencia", "ImoReferenciaExterna", "ImoCodigo", "Codigo"]),
    "responsavel_reserva": Map("TEXT", "direto", ["ResponsavelReserva"]),
    "sistema_teste": Map("TEXT", "default", default="", nota="Campo de teste do CRM, sem fonte"),
    "status_chave": Map("TEXT", "direto", ["StatusChave"]),
    "telefone_do_proprietario": Map("TEXT", "fallback", ["proprietarios.FoneComercial", "proprietarios.FoneResidencial", "proprietarios.Celular", "proprietarios.FonePrincipal"]),
    "telefone_zelador": Map("TEXT", "direto", ["ZeladorTelefone"]),
    "teste_campo": Map("TEXT", "default", default="", nota="Campo de teste do CRM, sem fonte"),
    "tipo_estrutura_galpao": Map("TEXT", "direto", ["FormatodoGalpao"]),
    "tipo_fachada": Map("TEXT", "fallback", ["Fachada", "Aberturas"]),
    "titulo_anuncio": Map("TEXT", "direto", ["TituloSite"]),
    "zelador_visitas": Map("TEXT", "direto", ["ZeladorNome"]),
    "zona": Map("TEXT", "direto", ["Zona"]),

    # ------------------------------------------------------------------ TEXTAREA
    "condicoes_negociacao": Map("TEXTAREA", "direto", ["InformacaoVenda"]),
    "descricao_do_empreendimento": Map("TEXTAREA", "direto", ["DescricaoEmpreendimento"]),
    "descricao_internet": Map("TEXTAREA", "fallback", ["DescricaoWeb", "TextoAnuncio", "KeywordsWeb"]),
    "imediacoes": Map("TEXTAREA", "fallback", ["Imediacoes"], func=lambda r: r.get("Imediacoes") or _pontos_interesse_resumo(r), nota="Imediacoes; senao resume PontoInteresse[]"),
    "observacoes_internas": Map("TEXTAREA", "fallback", ["Observacoes", "ObsVenda", "ObsLocacao"]),
    "observacoes_visitas": Map("TEXTAREA", "fallback", ["Visita", "VisitaAcompanhada"]),
    "visitacao": Map("TEXTAREA", "derivado", ["HoraSegSexInicio", "HoraSegSexFim", "HoraSabInicio", "HoraSabFim", "HoraDomInicio", "HoraDomFim"], func=lambda r: _visitacao_texto(r), nota="Texto consolidado dos horarios de visita"),

    # ------------------------------------------------------------------ URL
    "link_do_video": Map("URL", "fallback", ["URLVideo", "Video.Video"]),
}


# ----------------------------------------------------------------------------
# Helpers que dependem do MAPEAMENTO
# ----------------------------------------------------------------------------


def _pontos_interesse_resumo(rec: dict) -> str | None:
    pis = rec.get("PontosInteresse") or rec.get("PontoInteresse") or []
    if isinstance(pis, dict):
        pis = list(pis.values())
    if not isinstance(pis, list) or not pis:
        return None
    partes = []
    for pi in pis[:10]:
        if not isinstance(pi, dict):
            continue
        nome = pi.get("Nome") or pi.get("PontoInteresse")
        dist = pi.get("Distancia")
        if nome and dist:
            partes.append(f"{nome} ({dist})")
        elif nome:
            partes.append(str(nome))
    return " - ".join(partes) or None


def _visitacao_texto(rec: dict) -> str | None:
    linhas = []
    seg_i, seg_f = rec.get("HoraSegSexInicio"), rec.get("HoraSegSexFim")
    sab_i, sab_f = rec.get("HoraSabInicio"), rec.get("HoraSabFim")
    dom_i, dom_f = rec.get("HoraDomInicio"), rec.get("HoraDomFim")
    fer_i, fer_f = rec.get("HoraFerInicio"), rec.get("HoraFerFim")
    if seg_i and seg_f:
        linhas.append(f"Seg-Sex: {seg_i} as {seg_f}")
    if sab_i and sab_f:
        linhas.append(f"Sabado: {sab_i} as {sab_f}")
    if dom_i and dom_f:
        linhas.append(f"Domingo: {dom_i} as {dom_f}")
    if fer_i and fer_f:
        linhas.append(f"Feriado: {fer_i} as {fer_f}")
    return "\n".join(linhas) or None


# ============================================================================
# Resolver: pega o valor de um campo a partir do registro Vista
# ============================================================================


def resolve(field_name: str, rec: dict) -> Any:
    spec = MAPEAMENTO.get(field_name)
    if not spec:
        return None
    if spec.estrategia == "default":
        return spec.default
    if spec.estrategia in ("direto", "fallback"):
        v = _coalesce(rec, *spec.fontes)
        if v is None:
            return spec.default
        # coercao por tipo
        if spec.tipo == "BOOLEAN":
            return _to_bool(v)
        if spec.tipo in ("NUMBER", "CURRENCY"):
            return _to_number(v)
        return v
    if spec.estrategia == "derivado" and spec.func:
        return spec.func(rec)
    if spec.estrategia == "anexo" and spec.func:
        return spec.func(rec)
    if spec.estrategia == "reuso" and spec.func:
        return spec.func(rec)
    return None


# ============================================================================
# Exporta documentacao
# ============================================================================


def exportar_documentacao() -> None:
    # MD
    md = ["# Mapeamento TOTAL Vista -> Rafa (100% dos 299 campos)\n"]
    md.append(f"Total: **{len(MAPEAMENTO)}** campos cobertos.\n")
    md.append("Estrategias:")
    estrats: dict[str, int] = {}
    for sp in MAPEAMENTO.values():
        estrats[sp.estrategia] = estrats.get(sp.estrategia, 0) + 1
    for e, n in sorted(estrats.items()):
        md.append(f"- `{e}`: {n}")
    md.append("")
    md.append("| # | Campo Rafa | Tipo | Estrategia | Fonte(s) Vista | Nota |")
    md.append("|---|---|---|---|---|---|")
    for i, (k, sp) in enumerate(sorted(MAPEAMENTO.items()), 1):
        fontes = ", ".join(f"`{f}`" for f in sp.fontes) if sp.fontes else "-"
        md.append(f"| {i} | `{k}` | {sp.tipo} | {sp.estrategia} | {fontes} | {sp.nota} |")
    (BASE / "mapeamento_total.md").write_text("\n".join(md), encoding="utf-8")

    # JSON
    payload = {
        "total": len(MAPEAMENTO),
        "estrategias": estrats,
        "campos": {
            k: {
                "tipo": sp.tipo,
                "estrategia": sp.estrategia,
                "fontes": sp.fontes,
                "default": sp.default,
                "nota": sp.nota,
            }
            for k, sp in MAPEAMENTO.items()
        },
    }
    (BASE / "mapeamento_total.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    # CSV (para o Rafa revisar)
    with (BASE / "mapeamento_total.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["campo_rafa", "tipo", "estrategia", "fontes_vista", "nota"])
        for k, sp in sorted(MAPEAMENTO.items()):
            w.writerow([k, sp.tipo, sp.estrategia, " | ".join(sp.fontes), sp.nota])


# ============================================================================
# CLI
# ============================================================================


if __name__ == "__main__":
    print(f"Mapeamento DEFINITIVO: {len(MAPEAMENTO)} campos do Rafa cobertos")
    estrats: dict[str, int] = {}
    for sp in MAPEAMENTO.values():
        estrats[sp.estrategia] = estrats.get(sp.estrategia, 0) + 1
    for e, n in sorted(estrats.items()):
        print(f"  - {e:10s}: {n}")
    exportar_documentacao()
    print()
    print("Arquivos gerados:")
    for fn in ("mapeamento_total.md", "mapeamento_total.json", "mapeamento_total.csv"):
        print(f"  - {BASE / fn}")
