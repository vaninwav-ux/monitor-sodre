#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO - MONITOR COM TIMING MELHORADO
✅ 7 segundos de espera inicial
✅ Feedback em tempo real da interceptação
✅ Logs detalhados por seção
"""

import os
import asyncio
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional
from supabase import create_client, Client
from playwright.async_api import async_playwright


class SodreMonitor:
    """Monitor para Sodré Santoro - interceptação passiva e match com base"""
    
    def __init__(self):
        """Inicializa monitor"""
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("❌ Variáveis SUPABASE_URL e SUPABASE_KEY são obrigatórias")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        self.source = 'sodre'
        self.base_url = 'https://www.sodresantoro.com.br'
        self.leilao_base_url = 'https://leilao.sodresantoro.com.br'
        
        self.urls = [
            f"{self.base_url}/veiculos/lotes?sort=auction_date_init_asc",
            f"{self.base_url}/imoveis/lotes?sort=auction_date_init_asc",
            f"{self.base_url}/materiais/lotes?sort=auction_date_init_asc",
            f"{self.base_url}/sucatas/lotes?sort=auction_date_init_asc",
        ]
        
        self.stats = {
            'items_scraped': 0,
            'items_matched': 0,
            'items_new': 0,
            'snapshots_created': 0,
            'items_updated': 0,
            'bid_changes': 0,
            'visit_changes': 0,
            'status_changes': 0,
            'pages_scraped': 0,
            'errors': 0,
        }
        
        self.db_items_by_link = {}
        self.db_items_by_id = {}
        self.last_snapshots = {}
        self.section_counters = {}
    
    async def run(self):
        """Executa monitoramento completo"""
        print("\n" + "="*70)
        print("🟣 SODRÉ SANTORO - MONITORAMENTO")
        print("="*70)
        print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        start_time = time.time()
        
        print("\n📊 Carregando itens da base de dados...")
        self._load_database_items()
        print(f"✅ {len(self.db_items_by_link)} itens carregados da base")
        
        print("\n📸 Carregando últimos snapshots...")
        self._load_last_snapshots()
        print(f"✅ {len(self.last_snapshots)} snapshots anteriores carregados")
        
        print("\n🌐 Iniciando interceptação da API...")
        scraped_data = await self._scrape_with_interception()
        print(f"✅ {len(scraped_data)} lotes capturados")
        
        print("\n🔄 Processando matches e mudanças...")
        self._process_matches_and_snapshots(scraped_data)
        
        elapsed = time.time() - start_time
        self._print_stats(elapsed)
    
    def _load_database_items(self):
        """Carrega todos os itens ativos da base em memória"""
        try:
            response = self.supabase.schema('auctions').table('sodre_items') \
                .select('*') \
                .eq('source', 'sodre') \
                .execute()
            
            if response.data:
                for item in response.data:
                    link = item.get('link', '').split('?')[0].rstrip('/')
                    self.db_items_by_link[link] = item
                    self.db_items_by_id[item['id']] = item
            
        except Exception as e:
            print(f"❌ Erro ao carregar itens da base: {e}")
            raise
    
    def _load_last_snapshots(self):
        """Carrega último snapshot de cada item"""
        try:
            if not self.db_items_by_id:
                return
            
            item_ids = list(self.db_items_by_id.keys())
            
            for i in range(0, len(item_ids), 1000):
                batch = item_ids[i:i+1000]
                
                response = self.supabase.schema('auctions').table('sodre_monitoring') \
                    .select('*') \
                    .in_('item_id', batch) \
                    .order('snapshot_at', desc=True) \
                    .execute()
                
                if response.data:
                    seen = set()
                    for snap in response.data:
                        item_id = snap['item_id']
                        if item_id not in seen:
                            self.last_snapshots[item_id] = snap
                            seen.add(item_id)
        
        except Exception as e:
            print(f"⚠️ Erro ao carregar snapshots: {e}")
    
    async def _scrape_with_interception(self) -> List[Dict]:
        """Scrape via interceptação passiva da API - COM TIMING MELHORADO"""
        all_lots = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                locale='pt-BR'
            )
            
            page = await context.new_page()
            
            current_section = {'name': None}
            
            async def intercept_response(response):
                try:
                    if '/api/search-lots' in response.url and response.status == 200:
                        data = await response.json()
                        per_page = data.get('perPage', 0)
                        
                        if per_page > 0:
                            results = data.get('results', [])
                            hits = data.get('hits', {}).get('hits', [])
                            
                            lots_captured = 0
                            
                            if results:
                                all_lots.extend(results)
                                lots_captured = len(results)
                            elif hits:
                                extracted = [hit.get('_source', hit) for hit in hits]
                                all_lots.extend(extracted)
                                lots_captured = len(extracted)
                            
                            # ✅ Feedback em tempo real
                            if lots_captured > 0 and current_section['name']:
                                section = current_section['name']
                                if section not in self.section_counters:
                                    self.section_counters[section] = 0
                                self.section_counters[section] += lots_captured
                                print(f"     📥 +{lots_captured} lotes | Total seção: {self.section_counters[section]}")
                except:
                    pass
            
            page.on('response', intercept_response)
            
            for url in self.urls:
                section_name = url.split('/')[3]
                current_section['name'] = section_name
                lots_before = len(all_lots)
                
                print(f"\n📦 Processando: {section_name.upper()}")
                
                try:
                    await page.goto(url, wait_until="networkidle", timeout=60000)
                    
                    # ✅ ESPERA AUMENTADA: 7 segundos
                    print(f"  ⏳ Aguardando carregamento inicial (7s)...")
                    await asyncio.sleep(7)
                    
                    # Verifica captura inicial
                    initial_capture = len(all_lots) - lots_before
                    if initial_capture > 0:
                        print(f"  ✅ Página 1: {initial_capture} lotes capturados")
                    
                    # Paginação
                    for page_num in range(2, 51):
                        try:
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await asyncio.sleep(2)
                            
                            button = page.locator('button[title="Avançar"]:not([disabled])').first
                            if await button.count() > 0:
                                await button.click()
                                print(f"  ➡️  Página {page_num}...")
                                
                                # ✅ ESPERA APÓS CLICK: 5 segundos
                                await asyncio.sleep(5)
                                self.stats['pages_scraped'] += 1
                            else:
                                print(f"  ✅ {page_num-1} páginas processadas")
                                break
                        except:
                            break
                
                except Exception as e:
                    print(f"  ⚠️ Erro na seção {section_name}: {e}")
                
                lots_after = len(all_lots)
                section_lots = lots_after - lots_before
                
                if section_lots == 0:
                    print(f"  ⚠️  Nenhum lote capturado nesta seção")
                else:
                    print(f"  ✅ TOTAL DA SEÇÃO: {section_lots} lotes")
            
            await browser.close()
        
        self.stats['items_scraped'] = len(all_lots)
        return all_lots
    
    def _process_matches_and_snapshots(self, scraped_data: List[Dict]):
        """Processa matches com base e gera snapshots"""
        snapshots_batch = []
        updates_batch = []
        
        for scraped_lot in scraped_data:
            auction_id = scraped_lot.get('auction_id')
            lot_id = scraped_lot.get('lot_id')
            
            if not auction_id or not lot_id:
                continue
            
            link = f"{self.leilao_base_url}/leilao/{auction_id}/lote/{lot_id}/"
            link_clean = link.rstrip('/')
            
            db_item = self.db_items_by_link.get(link_clean)
            
            if not db_item:
                self.stats['items_new'] += 1
                continue
            
            self.stats['items_matched'] += 1
            
            last_snap = self.last_snapshots.get(db_item['id'])
            
            snapshot = self._create_snapshot(db_item, scraped_lot, last_snap)
            
            if snapshot:
                snapshots_batch.append(snapshot)
                self.stats['snapshots_created'] += 1
                
                if snapshot['bid_status_changed']:
                    self.stats['bid_changes'] += 1
                if snapshot.get('visit_increment') and snapshot['visit_increment'] > 0:
                    self.stats['visit_changes'] += 1
                if snapshot['status_changed']:
                    self.stats['status_changes'] += 1
            
            update = self._create_full_update(db_item, scraped_lot)
            if update:
                updates_batch.append(update)
        
        if snapshots_batch:
            print(f"\n💾 Inserindo {len(snapshots_batch)} snapshots...")
            self._insert_snapshots_batch(snapshots_batch)
        
        if updates_batch:
            print(f"\n🔄 Atualizando {len(updates_batch)} itens na tabela base...")
            self._update_base_items_batch(updates_batch)
    
    def _create_snapshot(self, db_item: Dict, scraped_lot: Dict, last_snap: Optional[Dict]) -> Optional[Dict]:
        """Cria snapshot de monitoramento"""
        try:
            now = datetime.now(timezone.utc)
            
            bid_initial = self._parse_numeric(scraped_lot.get('bid_initial'))
            bid_actual = self._parse_numeric(scraped_lot.get('bid_actual'))
            has_bid = bool(scraped_lot.get('bid_has_bid', False))
            lot_visits = self._parse_int(scraped_lot.get('lot_visits')) or 0
            lot_status = self._safe_str(scraped_lot.get('lot_status'))
            auction_status = self._safe_str(scraped_lot.get('auction_status'))
            
            auction_date_init = self._parse_datetime(scraped_lot.get('auction_date_init'))
            auction_date_end = self._parse_datetime(scraped_lot.get('auction_date_end'))
            
            hours_until_auction_start = None
            hours_since_auction_start = None
            days_in_auction = None
            
            if auction_date_init:
                try:
                    auction_dt = datetime.fromisoformat(auction_date_init)
                    delta = auction_dt - now
                    
                    if delta.total_seconds() > 0:
                        hours_until_auction_start = delta.total_seconds() / 3600
                    else:
                        hours_since_auction_start = abs(delta.total_seconds()) / 3600
                        days_in_auction = abs(delta.days)
                except:
                    pass
            
            old_bid_actual = last_snap['bid_actual'] if last_snap else db_item.get('bid_actual')
            old_has_bid = last_snap['has_bid'] if last_snap else db_item.get('has_bid', False)
            old_lot_visits = last_snap['lot_visits'] if last_snap else db_item.get('lot_visits', 0)
            old_lot_status = last_snap['lot_status'] if last_snap else db_item.get('lot_status')
            old_auction_status = last_snap['auction_status'] if last_snap else db_item.get('auction_status')
            
            bid_increment = None
            bid_increment_percentage = None
            bid_total_increment = None
            bid_total_increment_percentage = None
            
            if bid_actual:
                if old_bid_actual:
                    bid_increment = bid_actual - old_bid_actual
                    if old_bid_actual > 0:
                        bid_increment_percentage = (bid_increment / old_bid_actual) * 100
                
                if bid_initial and bid_initial > 0:
                    bid_total_increment = bid_actual - bid_initial
                    bid_total_increment_percentage = (bid_total_increment / bid_initial) * 100
            
            visit_increment = None
            if lot_visits and old_lot_visits is not None:
                visit_increment = lot_visits - old_lot_visits
            
            bid_status_changed = has_bid != old_has_bid
            lot_status_changed = lot_status != old_lot_status
            auction_status_changed = auction_status != old_auction_status
            status_changed = lot_status_changed or auction_status_changed
            
            is_active = auction_status == 'aberto' if auction_status else True
            
            total_snapshots_count = 1
            if last_snap:
                total_snapshots_count = last_snap.get('total_snapshots_count', 0) + 1
            
            snapshot = {
                'item_id': db_item['id'],
                'external_id': db_item['external_id'],
                'snapshot_at': now.isoformat(),
                'hours_until_auction_start': hours_until_auction_start,
                'hours_since_auction_start': hours_since_auction_start,
                'auction_date_init': auction_date_init,
                'auction_date_end': auction_date_end,
                'bid_initial': bid_initial,
                'bid_actual': bid_actual,
                'bid_increment': bid_increment,
                'bid_increment_percentage': bid_increment_percentage,
                'bid_total_increment': bid_total_increment,
                'bid_total_increment_percentage': bid_total_increment_percentage,
                'has_bid': has_bid,
                'bid_status_changed': bid_status_changed,
                'lot_visits': lot_visits,
                'visit_increment': visit_increment,
                'lot_status': lot_status,
                'lot_status_changed': lot_status_changed,
                'auction_status': auction_status,
                'auction_status_changed': auction_status_changed,
                'is_active': is_active,
                'status_changed': status_changed,
                'category': db_item.get('category'),
                'lot_category': db_item.get('lot_category'),
                'total_snapshots_count': total_snapshots_count,
                'metadata': {'source': 'automated_monitoring'}
            }
            
            return snapshot
        
        except Exception as e:
            self.stats['errors'] += 1
            return None
    
    def _create_full_update(self, db_item: Dict, scraped_lot: Dict) -> Optional[Dict]:
        """Cria update COMPLETO para tabela base"""
        try:
            now = datetime.now(timezone.utc).isoformat()
            
            auction_date_init = self._parse_datetime(scraped_lot.get('auction_date_init'))
            auction_date_2 = self._parse_datetime(scraped_lot.get('auction_date_2'))
            auction_date_end = self._parse_datetime(scraped_lot.get('auction_date_end'))
            
            image_url = None
            lot_pictures = scraped_lot.get('lot_pictures')
            if lot_pictures:
                if isinstance(lot_pictures, list) and len(lot_pictures) > 0:
                    image_url = lot_pictures[0]
                elif isinstance(lot_pictures, str):
                    image_url = lot_pictures
            
            lot_optionals = scraped_lot.get('lot_optionals')
            if lot_optionals:
                if isinstance(lot_optionals, list):
                    lot_optionals = [str(opt) for opt in lot_optionals if opt]
                elif isinstance(lot_optionals, str):
                    lot_optionals = [lot_optionals]
                else:
                    lot_optionals = None
            else:
                lot_optionals = None
            
            metadata = {
                'segment_base': scraped_lot.get('segment_base'),
                'lot_pictures': lot_pictures if lot_pictures else None,
                'search_terms': scraped_lot.get('search_terms'),
            }
            
            update = {
                'id': db_item['id'],
                'bid_initial': self._parse_numeric(scraped_lot.get('bid_initial')),
                'bid_actual': self._parse_numeric(scraped_lot.get('bid_actual')),
                'bid_has_bid': bool(scraped_lot.get('bid_has_bid', False)),
                'has_bid': bool(scraped_lot.get('bid_has_bid', False)),
                'lot_visits': self._parse_int(scraped_lot.get('lot_visits')),
                'lot_status': self._safe_str(scraped_lot.get('lot_status')),
                'auction_status': self._safe_str(scraped_lot.get('auction_status')),
                'is_active': scraped_lot.get('auction_status') == 'aberto',
                'auction_name': self._safe_str(scraped_lot.get('auction_name')),
                'auction_date_init': auction_date_init,
                'auction_date_2': auction_date_2,
                'auction_date_end': auction_date_end,
                'auctioneer_name': self._safe_str(scraped_lot.get('auctioneer_name')),
                'client_id': self._parse_int(scraped_lot.get('client_id')),
                'client_name': self._safe_str(scraped_lot.get('client_name')),
                'bid_user_nickname': self._safe_str(scraped_lot.get('bid_user_nickname')),
                'lot_brand': self._safe_str(scraped_lot.get('lot_brand')),
                'lot_model': self._safe_str(scraped_lot.get('lot_model')),
                'lot_year_manufacture': self._parse_int(scraped_lot.get('lot_year_manufacture')),
                'lot_year_model': self._parse_int(scraped_lot.get('lot_year_model')),
                'lot_plate': self._safe_str(scraped_lot.get('lot_plate')),
                'lot_color': self._safe_str(scraped_lot.get('lot_color')),
                'lot_km': self._parse_int(scraped_lot.get('lot_km')),
                'lot_fuel': self._safe_str(scraped_lot.get('lot_fuel')),
                'lot_transmission': self._safe_str(scraped_lot.get('lot_transmission')),
                'lot_sinister': self._safe_str(scraped_lot.get('lot_sinister')),
                'lot_origin': self._safe_str(scraped_lot.get('lot_origin')),
                'lot_optionals': lot_optionals,
                'lot_tags': self._safe_str(scraped_lot.get('lot_tags')),
                'image_url': image_url,
                'lot_status_id': self._parse_int(scraped_lot.get('lot_status_id')),
                'lot_is_judicial': bool(scraped_lot.get('lot_is_judicial', False)),
                'lot_is_scrap': bool(scraped_lot.get('lot_is_scrap', False)),
                'lot_financeable': bool(scraped_lot.get('lot_status_financeable', False)),
                'is_highlight': bool(scraped_lot.get('is_highlight', False)),
                'lot_test': bool(scraped_lot.get('lot_test', False)),
                'lot_judicial_process': self._safe_str(scraped_lot.get('lot_judicial_process')),
                'lot_judicial_action': self._safe_str(scraped_lot.get('lot_judicial_action')),
                'lot_judicial_executor': self._safe_str(scraped_lot.get('lot_judicial_executor')),
                'lot_judicial_executed': self._safe_str(scraped_lot.get('lot_judicial_executed')),
                'lot_judicial_judge': self._safe_str(scraped_lot.get('lot_judicial_judge')),
                'tj_praca_value': self._parse_numeric(scraped_lot.get('tj_praca_value')),
                'tj_praca_discount': self._parse_numeric(scraped_lot.get('tj_praca_discount')),
                'lot_neighborhood': self._safe_str(scraped_lot.get('lot_neighborhood')),
                'lot_street': self._safe_str(scraped_lot.get('lot_street')),
                'lot_dormitories': self._parse_int(scraped_lot.get('lot_dormitories')),
                'lot_useful_area': self._parse_numeric(scraped_lot.get('lot_useful_area')),
                'lot_total_area': self._parse_numeric(scraped_lot.get('lot_total_area')),
                'lot_suites': self._parse_int(scraped_lot.get('lot_suites')),
                'lot_subcategory': self._safe_str(scraped_lot.get('lot_subcategory')),
                'lot_type_name': self._safe_str(scraped_lot.get('lot_type_name')),
                'metadata': {k: v for k, v in metadata.items() if v is not None},
                'updated_at': now,
                'last_scraped_at': now,
            }
            
            return update
        
        except Exception as e:
            self.stats['errors'] += 1
            return None
    
    def _insert_snapshots_batch(self, snapshots: List[Dict]):
        """Insere snapshots em lotes"""
        try:
            batch_size = 500
            for i in range(0, len(snapshots), batch_size):
                batch = snapshots[i:i+batch_size]
                self.supabase.schema('auctions').table('sodre_monitoring').insert(batch).execute()
                print(f"  ✅ Lote {i//batch_size + 1}: {len(batch)} snapshots inseridos")
        
        except Exception as e:
            print(f"❌ Erro ao inserir snapshots: {e}")
            self.stats['errors'] += len(snapshots)
    
    def _update_base_items_batch(self, updates: List[Dict]):
        """Atualiza tabela base em lotes"""
        try:
            for update in updates:
                try:
                    item_id = update.pop('id')
                    
                    self.supabase.schema('auctions').table('sodre_items') \
                        .update(update) \
                        .eq('id', item_id) \
                        .execute()
                    self.stats['items_updated'] += 1
                except Exception as e:
                    self.stats['errors'] += 1
            
            print(f"  ✅ {self.stats['items_updated']} itens atualizados")
        
        except Exception as e:
            print(f"❌ Erro ao atualizar itens: {e}")
    
    def _safe_str(self, value) -> str:
        if value is None:
            return None
        try:
            result = str(value).strip()
            return result if result else None
        except:
            return None
    
    def _parse_datetime(self, value) -> Optional[str]:
        if not value:
            return None
        try:
            if isinstance(value, str):
                value = value.replace('Z', '+00:00')
                if 'T' in value:
                    return value
                else:
                    dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    return dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            return None
        except:
            return None
    
    def _parse_numeric(self, value) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except:
            return None
    
    def _parse_int(self, value) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except:
            return None
    
    def _print_stats(self, elapsed: float):
        """Imprime estatísticas finais"""
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        print(f"\n{'='*70}")
        print("📊 ESTATÍSTICAS FINAIS")
        print(f"{'='*70}")
        print(f"\n  Scrape:")
        print(f"    • Páginas processadas: {self.stats['pages_scraped']}")
        print(f"    • Itens capturados: {self.stats['items_scraped']}")
        print(f"\n  Match:")
        print(f"    • Itens encontrados na base: {self.stats['items_matched']}")
        print(f"    • Itens novos (não na base): {self.stats['items_new']}")
        print(f"\n  Monitoramento:")
        print(f"    • Snapshots criados: {self.stats['snapshots_created']}")
        print(f"    • Itens atualizados: {self.stats['items_updated']}")
        print(f"\n  Mudanças detectadas:")
        print(f"    • Mudanças de lances: {self.stats['bid_changes']}")
        print(f"    • Aumento de visitas: {self.stats['visit_changes']}")
        print(f"    • Mudanças de status: {self.stats['status_changes']}")
        
        if self.stats['errors'] > 0:
            print(f"\n  ⚠️ Erros: {self.stats['errors']}")
        
        print(f"\n⏱️ Duração: {minutes}min {seconds}s")
        print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")


async def main():
    """Execução principal"""
    try:
        monitor = SodreMonitor()
        await monitor.run()
    
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())