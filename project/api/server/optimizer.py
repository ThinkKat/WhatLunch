import pandas as pd
import numpy as np
import math
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import random
import json
from collections import defaultdict
from copy import deepcopy
import textwrap
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class AuctionItem:
    listing_id: str
    brand: str
    model: Optional[str] = None
    year: Optional[int] = None
    mileage_km: Optional[int] = None
    auction_house: str = None
    min_price: int = 0
    date: str = None
    transmission: Optional[str] = None
    fuel: Optional[str] = None
    color: Optional[str] = None
    displacement_cc: Optional[int] = None
    priority_score: float = 0.0

@dataclass
class AuctionState:
    """MCTS 노드의 상태를 나타내는 클래스"""
    current_budget: int
    remaining_targets: Dict[str, int]
    available_auctions: List[AuctionItem]
    current_inventory: Dict[str, int]
    time_step: int
    total_auctions: int = 0
    
    def is_terminal(self) -> bool:
        """터미널 상태 확인"""
        if self.current_budget <= 0:
            return True
        if len(self.available_auctions) == 0:
            return True
        if sum(self.remaining_targets.values()) <= 0:
            return True
        return False
    
    def get_model_key(self, auction_item: AuctionItem, level: int = 0) -> str:
        """계층적 모델 키 생성"""
        brand = auction_item.brand or "unknown"
        model = auction_item.model or "any"
        year = auction_item.year or 0
        
        if level == 0:  # 완전 매칭
            return f"{brand}_{model}_{year}"
        elif level == 1:  # 모델까지만
            return f"{brand}_{model}"
        elif level == 2:  # 브랜드만
            return f"{brand}"
        else:
            return "any"
    
    def find_matching_targets(self, auction_item: AuctionItem) -> List[Tuple[str, int]]:
        """경매 아이템에 매칭되는 목표들을 찾음"""
        matches = []
        
        for level in range(4):
            key = self.get_model_key(auction_item, level)
            if key in self.remaining_targets and self.remaining_targets[key] > 0:
                matches.append((key, level))
        
        matches.sort(key=lambda x: x[1])
        return matches
    
    def get_target_achievement_rate(self) -> float:
        """목표 달성률 계산"""
        total_targets = sum(self.current_inventory.values()) + sum(self.remaining_targets.values())
        achieved = sum(self.current_inventory.values())
        return achieved / total_targets if total_targets > 0 else 1.0

@dataclass
class AuctionAction:
    """MCTS 액션"""
    auction_id: str
    bid_amount: int
    action_type: str
    target_key: str = None

class MCTSNode:
    """MCTS 트리의 노드"""
    def __init__(self, state: AuctionState, action: AuctionAction = None, parent=None, optimizer=None):
        self.state = state
        self.action = action
        self.parent = parent
        self.optimizer = optimizer
        self.children = []
        self.visits = 0
        self.total_reward = 0.0
        self.untried_actions = []
        self._action_cache = {}
        self._initialize_actions()
    
    def _initialize_actions(self):
        """액션 초기화"""
        if self.state.is_terminal():
            return
            
        if not self.state.available_auctions:
            return
            
        current_auction = self.state.available_auctions[0]
        
        cache_key = f"{current_auction.listing_id}_{self.state.current_budget}"
        if cache_key in self._action_cache:
            self.untried_actions = self._action_cache[cache_key].copy()
            return
        
        actions = []
        matching_targets = self.state.find_matching_targets(current_auction)
        
        if matching_targets:
            if self.state.current_budget >= current_auction.min_price:
                max_affordable = min(self.state.current_budget, current_auction.min_price * 2.0)
                
                best_match_level = matching_targets[0][1]
                
                if best_match_level == 0:  # 완전 매칭
                    multipliers = [1.05, 1.1, 1.15, 1.2, 1.3]
                elif best_match_level == 1:  # 모델 매칭
                    multipliers = [1.08, 1.13, 1.18, 1.25]
                elif best_match_level == 2:  # 브랜드 매칭
                    multipliers = [1.12, 1.18, 1.25]
                else:  # 일반 매칭
                    multipliers = [1.15, 1.22]
                
                action_types = ["conservative", "moderate", "aggressive", "very_aggressive", "urgent"][:len(multipliers)]
                
                for multiplier, action_type in zip(multipliers, action_types):
                    bid_price = int(current_auction.min_price * multiplier)
                    if bid_price <= max_affordable:
                        target_key = matching_targets[0][0]
                        action = AuctionAction(current_auction.listing_id, bid_price, action_type, target_key)
                        actions.append(action)
                
                total_remaining = sum(self.state.remaining_targets.values())
                remaining_relevant = len([a for a in self.state.available_auctions[:10]
                                        if self.state.find_matching_targets(a)])
                
                if total_remaining <= remaining_relevant * 0.8:
                    actions.append(AuctionAction(current_auction.listing_id, 0, "skip"))
        else:
            actions.append(AuctionAction(current_auction.listing_id, 0, "skip"))
        
        self.untried_actions = actions
        self._action_cache[cache_key] = actions.copy()
    
    def is_fully_expanded(self) -> bool:
        return len(self.untried_actions) == 0
    
    def uct_value(self, c=1.414) -> float:
        """UCT 값 계산"""
        if self.visits == 0:
            return float('inf')
        
        exploitation = self.total_reward / self.visits
        exploration = c * math.sqrt(math.log(self.parent.visits) / self.visits)
        
        return exploitation + exploration
    
    def best_child(self, c=1.414):
        """최고의 자식 노드 선택"""
        if not self.children:
            raise ValueError("Node has no children")
        return max(self.children, key=lambda child: child.uct_value(c))
    
    def add_child(self, action: AuctionAction, state: AuctionState):
        """자식 노드 추가"""
        optimizer_ref = self.optimizer if self.optimizer else (self.parent.optimizer if self.parent else None)
        child = MCTSNode(state, action, self, optimizer_ref)
        self.children.append(child)
        return child

class MCTSAuctionOptimizer:
    """MCTS 경매 최적화 시스템"""
    
    def __init__(self, historical_data: pd.DataFrame, config: Dict = None):
        self.historical_data = historical_data
        self.current_date = datetime.now()
        
        default_config = {
            'lookahead_window': 100,
            'max_tree_depth': 15,
            'simulation_length': 20,
            'parallel_simulations': 8,
            'adaptive_iterations': True,
            'memory_limit_mb': 1000,
            'cache_size': 20000,
            'early_termination': True,
            'quality_threshold': 0.8,
            'brand_priority_weight': 1.0,
            'model_priority_weight': 0.8,
            'year_priority_weight': 0.6,
        }
        
        self.config = {**default_config, **(config or {})}
        
        if len(historical_data) > 10000:
            self.config.update({
                'simulation_length': 15,
                'max_tree_depth': 12,
                'cache_size': 50000,
            })
        
        self._preprocess_data()
        self._setup_caches()

    def _should_terminate_early(self, root: MCTSNode, iteration: int, total_iterations: int) -> bool:
        """조기 종료 조건 확인"""
        if not self.config.get('early_termination', False):
            return False
        
        if iteration < max(100, total_iterations * 0.1):
            return False
        
        if iteration % 50 == 0 and iteration > 200:
            if root.children:
                best_child = max(root.children, key=lambda c: c.visits)
                visit_ratio = best_child.visits / root.visits
                
                if visit_ratio > 0.8:
                    logger.info(f"조기 종료: iteration {iteration}에서 수렴 감지")
                    return True
        
        return False
        
    def _setup_caches(self):
        """캐시 초기화"""
        self._similarity_cache = {}
        self._probability_cache = {}
        self._price_cache = {}
        self._cache_hits = 0
        self._cache_misses = 0
        
    def _preprocess_data(self):
        """데이터 전처리"""
        logger.info("데이터 전처리 시작...")
        
        self.historical_data = self.historical_data.copy()
        self.historical_data['brand'] = self.historical_data['brand'].fillna('unknown')
        self.historical_data['model'] = self.historical_data['model'].fillna('any')
        self.historical_data['year'] = pd.to_numeric(self.historical_data['year'], errors='coerce').fillna(0).astype(int)
        self.historical_data['mileage_km'] = pd.to_numeric(self.historical_data['mileage_km'], errors='coerce').fillna(0).astype(int)

        self.historical_data["auction_date"] = pd.to_datetime(
            self.historical_data["auction_date"], format='mixed'
        )
        self.historical_data["age"] = np.maximum(0, 
            self.historical_data["auction_date"].dt.year - self.historical_data["year"]
        )

        grouping_columns = []
        for col in self.historical_data.columns:
            if col not in ['winning_price', 'auction_date', 'age']:
                grouping_columns.append(col)

        def q50(x): return x.quantile(0.5) if len(x) > 0 else 0
        def q75(x): return x.quantile(0.75) if len(x) > 0 else 0
        def q90(x): return x.quantile(0.9) if len(x) > 0 else 0
        
        try:
            self.processed_data = (
                self.historical_data.groupby(grouping_columns, dropna=False)
                .agg(
                    reliability=("winning_price", "count"),
                    min=("winning_price", "min"),
                    max=("winning_price", "max"),
                    mean=("winning_price", "mean"),
                    median=("winning_price", "median"),
                    std=("winning_price", "std"),
                    q50=("winning_price", q50),
                    q75=("winning_price", q75),
                    q90=("winning_price", q90),
                    price_list=("winning_price", lambda x: list(x)),
                    date_list=("auction_date", lambda x: list(x)),
                )
                .reset_index()
            )

            self.processed_data["price_range"] = self.processed_data["max"] - self.processed_data["min"]
            
            self._create_lookup_indices()
            
            logger.info(f"전처리 완료 - {len(self.processed_data)}개 차량 데이터")
            
        except Exception as e:
            logger.error(f"전처리 중 오류 발생: {e}")
            self.processed_data = pd.DataFrame()
            self._create_lookup_indices()
    
    def _create_lookup_indices(self):
        """계층적 검색 인덱스 생성"""
        self.brand_exact_index = defaultdict(list)
        self.brand_model_index = defaultdict(list)
        self.brand_only_index = defaultdict(list)
        
        if self.processed_data.empty:
            return
            
        for idx, row in self.processed_data.iterrows():
            brand = row.get('brand', 'unknown')
            model = row.get('model', 'any')
            year = row.get('year', 0)
            
            exact_key = f"{brand}_{model}_{year}"
            self.brand_exact_index[exact_key].append(idx)
            
            model_key = f"{brand}_{model}"
            self.brand_model_index[model_key].append(idx)
            
            self.brand_only_index[brand].append(idx)
    
    def _calculate_time_weights(self, dates: List[datetime]) -> List[float]:
        """시간 가중치 계산"""
        weights = []
        for date in dates:
            days_diff = (self.current_date - date).days
            time_weight = math.exp(-days_diff / 180)
            weights.append(min(time_weight, 1.0))
        return weights
    
    def _find_similar_cars(self, auction_item: AuctionItem) -> List[int]:
        """계층적 유사 차량 검색"""
        brand = auction_item.brand or "unknown"
        model = auction_item.model or "any"
        year = auction_item.year or 0
        
        cache_key = f"{brand}_{model}_{year}_{auction_item.mileage_km or 0}"
        
        if cache_key in self._similarity_cache:
            self._cache_hits += 1
            return self._similarity_cache[cache_key]
        
        self._cache_misses += 1
        similar_indices = []
        
        # 1단계: 완전 매칭
        exact_key = f"{brand}_{model}_{year}"
        candidates = self.brand_exact_index.get(exact_key, [])
        
        if candidates and len(candidates) >= 3:
            similar_indices = candidates[:20]
        else:
            # 2단계: 모델까지 매칭
            model_key = f"{brand}_{model}"
            model_candidates = self.brand_model_index.get(model_key, [])
            
            if model_candidates and len(model_candidates) >= 3:
                filtered = []
                for idx in model_candidates:
                    if idx < len(self.processed_data):
                        row = self.processed_data.iloc[idx]
                        row_year = row.get('year', 0)
                        if year == 0 or row_year == 0 or abs(year - row_year) <= 3:
                            filtered.append(idx)
                similar_indices = filtered[:15]
            
            if len(similar_indices) < 3:
                # 3단계: 브랜드만 매칭
                brand_candidates = self.brand_only_index.get(brand, [])
                
                if brand_candidates:
                    filtered = []
                    mileage = auction_item.mileage_km or 50000
                    
                    for idx in brand_candidates[:50]:
                        if idx < len(self.processed_data):
                            row = self.processed_data.iloc[idx]
                            row_year = row.get('year', 0)
                            row_mileage = row.get('mileage_km', 50000)
                            
                            year_ok = year == 0 or row_year == 0 or abs(year - row_year) <= 5
                            mileage_ok = abs(mileage - row_mileage) <= 50000
                            
                            if year_ok and mileage_ok:
                                filtered.append(idx)
                    
                    similar_indices.extend(filtered[:10])
        
        if not similar_indices and brand in self.brand_only_index:
            similar_indices = self.brand_only_index[brand][:5]
        
        if len(self._similarity_cache) >= self.config['cache_size']:
            old_keys = list(self._similarity_cache.keys())[:-self.config['cache_size']//2]
            for old_key in old_keys:
                del self._similarity_cache[old_key]
        
        self._similarity_cache[cache_key] = similar_indices
        return similar_indices
    
    def _calculate_win_probability(self, auction_item: AuctionItem, bid_price: float) -> float:
        """승률 계산"""
        cache_key = f"{auction_item.listing_id}_{int(bid_price)}"
        
        if cache_key in self._probability_cache:
            return self._probability_cache[cache_key]
        
        similar_indices = self._find_similar_cars(auction_item)
        
        if not similar_indices:
            min_price = auction_item.min_price or 1000000
            price_ratio = bid_price / min_price
            if price_ratio >= 1.25:
                prob = 0.85
            elif price_ratio >= 1.15:
                prob = 0.70
            elif price_ratio >= 1.12:
                prob = 0.55
            elif price_ratio >= 1.07:
                prob = 0.40
            elif price_ratio >= 1.05:
                prob = 0.25
            else:
                prob = 0.15
        else:
            sample_size = min(10, len(similar_indices))
            sampled_indices = random.sample(similar_indices, sample_size)
            
            all_prices = []
            for idx in sampled_indices:
                if idx < len(self.processed_data):
                    car_info = self.processed_data.iloc[idx]
                    price_list = car_info.get('price_list', [])
                    if price_list and isinstance(price_list, list):
                        all_prices.extend(price_list[:50])
            
            if all_prices:
                success_count = sum(1 for price in all_prices if price <= bid_price)
                prob = min(success_count / len(all_prices), 0.95)
            else:
                prob = 0.3
        
        self._probability_cache[cache_key] = prob
        return prob
    
    def _apply_action(self, state: AuctionState, action: AuctionAction) -> Tuple[AuctionState, float]:
        """액션 적용"""
        new_state = AuctionState(
            current_budget=state.current_budget,
            remaining_targets=state.remaining_targets.copy(),
            available_auctions=state.available_auctions[1:],
            current_inventory=state.current_inventory.copy(),
            time_step=state.time_step + 1,
            total_auctions=state.total_auctions
        )
        
        if not state.available_auctions:
            return new_state, 0
        
        current_auction = state.available_auctions[0]
        
        if action.bid_amount > 0:
            win_prob = self._calculate_win_probability(current_auction, action.bid_amount)
            
            if random.random() < win_prob:
                actual_price = int(action.bid_amount * random.uniform(0.85, 1.0))
                actual_price = max(actual_price, current_auction.min_price or 0)
                
                new_state.current_budget -= actual_price
                
                if action.target_key and action.target_key in new_state.remaining_targets:
                    new_state.current_inventory[action.target_key] = new_state.current_inventory.get(action.target_key, 0) + 1
                    new_state.remaining_targets[action.target_key] = max(0, new_state.remaining_targets[action.target_key] - 1)
                else:
                    matching_targets = state.find_matching_targets(current_auction)
                    if matching_targets:
                        target_key = matching_targets[0][0]
                        new_state.current_inventory[target_key] = new_state.current_inventory.get(target_key, 0) + 1
                        new_state.remaining_targets[target_key] = max(0, new_state.remaining_targets[target_key] - 1)
        
        return new_state, 0
    
    def _simulate(self, state: AuctionState) -> float:
        """시뮬레이션"""
        current_state = deepcopy(state)
        simulation_steps = 0
        max_steps = min(self.config['simulation_length'], len(current_state.available_auctions))
        
        while (not current_state.is_terminal() and 
               simulation_steps < max_steps and
               current_state.available_auctions):
            
            current_auction = current_state.available_auctions[0]
            matching_targets = current_state.find_matching_targets(current_auction)
            
            if (matching_targets and 
                current_state.current_budget >= (current_auction.min_price or 0)):
                
                multiplier = random.uniform(1.1, 1.5)
                max_bid = min(current_state.current_budget, 
                             int((current_auction.min_price or 1000000) * multiplier))
                target_key = matching_targets[0][0]
                action = AuctionAction(current_auction.listing_id, max_bid, "simulation", target_key)
            else:
                action = AuctionAction(current_auction.listing_id, 0, "skip")
            
            current_state, _ = self._apply_action(current_state, action)
            simulation_steps += 1
        
        total_purchased = sum(current_state.current_inventory.values())
        base_reward = total_purchased * 10.0
        
        total_targets = sum(state.remaining_targets.values())
        if total_targets > 0:
            achievement_rate = total_purchased / total_targets
            if achievement_rate >= 1.0:
                achievement_bonus = 50.0
            elif achievement_rate >= 0.8:
                achievement_bonus = 20.0
            else:
                achievement_bonus = 0
        else:
            achievement_bonus = 0
        
        return base_reward + achievement_bonus
    
    def _create_purchase_targets(self, purchase_plans: List[Dict]) -> Dict[str, int]:
        """구매 목표 생성"""
        targets = {}
        
        for plan in purchase_plans:
            brand = plan.get('brand', 'any')
            model = plan.get('model')
            year = plan.get('year')
            target_units = plan.get('target_units', 0)
            
            if brand and model and year:
                key = f"{brand}_{model}_{year}"
            elif brand and model:
                key = f"{brand}_{model}"
            elif brand:
                key = f"{brand}"
            else:
                key = "any"
            
            targets[key] = targets.get(key, 0) + target_units
        
        return targets
    
    def _prioritize_auctions(self, auctions: List[AuctionItem], targets: Dict[str, int]) -> List[AuctionItem]:
        """경매 우선순위 정렬"""
        def get_priority(auction):
            matching_targets = []
            for target_key, target_count in targets.items():
                if target_count <= 0:
                    continue
                    
                if self._matches_target(auction, target_key):
                    if "_" in target_key:
                        parts = target_key.split("_")
                        if len(parts) == 3:
                            weight = 100
                        elif len(parts) == 2:
                            weight = 80
                        else:
                            weight = 60
                    else:
                        weight = 40
                        
                    priority = target_count * weight
                    matching_targets.append(priority)
            
            if not matching_targets:
                return 0
                
            max_priority = max(matching_targets)
            price_priority = 1000000 / max(auction.min_price or 1000000, 1)
            
            return max_priority + price_priority
        
        return sorted(auctions, key=get_priority, reverse=True)
    
    def _matches_target(self, auction: AuctionItem, target_key: str) -> bool:
        """경매 아이템이 목표와 매칭되는지 확인"""
        if target_key == "any":
            return True
            
        parts = target_key.split("_")
        auction_brand = auction.brand or "unknown"
        auction_model = auction.model or "any" 
        auction_year = auction.year or 0
        
        if len(parts) == 1:
            return auction_brand == parts[0]
        elif len(parts) == 2:
            return (auction_brand == parts[0] and 
                   (auction_model == parts[1] or parts[1] == "any"))
        elif len(parts) == 3:
            return (auction_brand == parts[0] and 
                   (auction_model == parts[1] or parts[1] == "any") and
                   (auction_year == int(parts[2]) or int(parts[2]) == 0))
        
        return False
    
    def mcts_search(self, initial_state: AuctionState, iterations: int = 1000) -> MCTSNode:
        """MCTS 검색"""
        initial_state.available_auctions = self._prioritize_auctions(
            initial_state.available_auctions, initial_state.remaining_targets
        )
        
        if len(initial_state.available_auctions) > self.config['lookahead_window']:
            windowed_auctions = initial_state.available_auctions[:self.config['lookahead_window']]
            initial_state.available_auctions = windowed_auctions
            logger.info(f"Lookahead window 적용: {len(windowed_auctions)}개 경매로 제한")
        
        root = MCTSNode(initial_state, optimizer=self)
        
        if self.config['adaptive_iterations']:
            auction_count = len(initial_state.available_auctions)
            base_iter = iterations
            
            if auction_count > 200:
                scale_factor = max(0.3, math.log10(200) / math.log10(auction_count))
                iterations = max(500, int(base_iter * scale_factor))
            elif auction_count > 100:
                scale_factor = max(0.7, 100 / auction_count)
                iterations = max(800, int(base_iter * scale_factor))
            else:
                iterations = min(base_iter, 2000)
        
        logger.info(f"MCTS 검색 시작 - iterations={iterations}, auctions={len(initial_state.available_auctions)}")
    
        progress_interval = iterations
        
        for i in range(iterations):
            if i % 100 == 0 and i > 0:
                if len(self._similarity_cache) > self.config['cache_size']:
                    old_keys = list(self._similarity_cache.keys())[:-self.config['cache_size']//2]
                    for key in old_keys:
                        del self._similarity_cache[key]
            
            # Selection
            node = root
            depth = 0
            while (not node.state.is_terminal() and 
                   node.is_fully_expanded() and 
                   node.children and 
                   depth < self.config['max_tree_depth']):
                node = node.best_child()
                depth += 1
            
            # Expansion
            if (not node.state.is_terminal() and 
                not node.is_fully_expanded() and
                depth < self.config['max_tree_depth']):
                if node.untried_actions:
                    action = random.choice(node.untried_actions)
                    node.untried_actions.remove(action)
                    new_state, _ = self._apply_action(node.state, action)
                    node = node.add_child(action, new_state)
            
            # Simulation
            reward = self._simulate(node.state)
            
            # Backpropagation
            while node is not None:
                node.visits += 1
                node.total_reward += reward
                node = node.parent
            
            if (i + 1) % progress_interval == 0:
                cache_hit_rate = self._cache_hits / max(self._cache_hits + self._cache_misses, 1) * 100
                logger.info(f"MCTS 진행: {i+1}/{iterations} ({(i+1)/iterations*100:.1f}%) "
                           f"- 캐시 적중률: {cache_hit_rate:.1f}%")

        logger.info(f"MCTS 검색 완료 - Root visits={root.visits}, children={len(root.children)}")
        return root
    
    def get_best_action_sequence(self, root: MCTSNode, max_depth: int = None) -> List[AuctionAction]:
        """최적 액션 시퀀스 추출"""
        if max_depth is None:
            max_depth = min(20, len(root.state.available_auctions))
        
        sequence = []
        node = root
        depth = 0
        
        while node.children and depth < max_depth:
            best_child = max(node.children, key=lambda child: child.visits)
            if best_child.action:
                sequence.append(best_child.action)
            node = best_child
            depth += 1
        
        return sequence
    
    def optimize_auction_strategy(self, optimization_input: Dict, iterations: int = 1000) -> Dict:
        """경매 전략 최적화"""
        start_time = datetime.now()
        
        budget = optimization_input.get('budget', 0)
        purchase_plans = optimization_input.get('purchase_plans', [])
        auction_schedule = optimization_input.get('auction_schedule', [])
        
        logger.info(f"최적화 시작 - 예산: {budget:,}원, 경매: {len(auction_schedule)}개")
        
        remaining_targets = self._create_purchase_targets(purchase_plans)
        logger.info(f"구매 목표: {remaining_targets}")
        
        # 경매 아이템 생성
        all_auctions = []
        for i, item in enumerate(auction_schedule):
            auction_item = AuctionItem(
                listing_id=item.get('listing_id', f"auction_{i}"),
                brand=item.get('brand'),
                model=item.get('model'),
                year=item.get('year'),
                mileage_km=item.get('mileage_km'),
                auction_house=item.get('auction_house'),
                min_price=item.get('min_price', 0),
                date=item.get('date'),
                transmission=item.get('transmission'),
                fuel=item.get('fuel'),
                color=item.get('color'),
                displacement_cc=item.get('displacement_cc')
            )
            all_auctions.append(auction_item)
        
        all_auctions = self._prioritize_auctions(all_auctions, remaining_targets)
        
        # Rolling Horizon 처리
        current_budget = budget
        current_inventory = {}
        all_executed_actions = []
        processed_auctions = 0
        
        window_size = self.config['lookahead_window']
        total_batches = (len(all_auctions) + window_size - 1) // window_size
        
        logger.info(f"Rolling Horizon 처리: {total_batches}개 배치로 분할")
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * window_size
            end_idx = min((batch_idx + 1) * window_size, len(all_auctions))
            current_batch = all_auctions[start_idx:end_idx]
            
            if not current_batch or current_budget <= 0:
                break

            if total_batches <= 10:
                batch_iterations = max(1000, iterations // 2)
            elif total_batches <= 50:
                batch_iterations = max(600, iterations // 3)
            elif total_batches <= 200:
                batch_iterations = max(400, iterations // 5)
            else:
                batch_iterations = max(300, iterations // 8)
            
            if batch_idx < min(5, total_batches // 4):
                batch_iterations = int(batch_iterations * 1.5)
                
            logger.info(f"배치 {batch_idx + 1}/{total_batches} 처리 중... "
                       f"({len(current_batch)}개 경매, 예산: {current_budget:,}원)")
            
            batch_state = AuctionState(
                current_budget=current_budget,
                remaining_targets=remaining_targets.copy(),
                available_auctions=current_batch,
                current_inventory=current_inventory.copy(),
                time_step=processed_auctions,
                total_auctions=len(all_auctions)
            )
            
            batch_root = self.mcts_search(batch_state, batch_iterations)
            batch_actions = self.get_best_action_sequence(batch_root, len(current_batch))
            
            # 시뮬레이션
            batch_simulation_runs = 20
            batch_results = []
            
            for _ in range(batch_simulation_runs):
                sim_state = deepcopy(batch_state)
                sim_actions = []
                sim_cost = 0
                
                for action in batch_actions:
                    if sim_state.is_terminal() or not sim_state.available_auctions:
                        break
                        
                    current_auction = sim_state.available_auctions[0]
                    
                    if action.bid_amount > 0:
                        win_prob = self._calculate_win_probability(current_auction, action.bid_amount)
                        if random.random() < win_prob:
                            actual_price = int(action.bid_amount * random.uniform(0.85, 1.0))
                            actual_price = max(actual_price, current_auction.min_price or 0)
                            
                            sim_actions.append({
                                'auction_house': current_auction.auction_house or 'unknown',
                                'listing_id': current_auction.listing_id,
                                'brand': current_auction.brand or 'unknown',
                                'model': current_auction.model or 'any',
                                'year': current_auction.year or 0,
                                'max_bid_price': action.bid_amount,
                                'expected_price': actual_price,
                                'auction_end_date': current_auction.date,
                                'action_type': action.action_type,
                                'target_key': action.target_key,
                                'win_probability': round(win_prob, 3)
                            })
                            
                            sim_cost += actual_price
                    
                    sim_state, _ = self._apply_action(sim_state, action)
                
                batch_results.append({
                    'actions': sim_actions,
                    'cost': sim_cost,
                    'inventory': dict(sim_state.current_inventory)
                })
            
            # 최적 배치 결과 선택
            avg_cost = np.mean([r['cost'] for r in batch_results])
            best_result_idx = min(range(len(batch_results)), 
                                key=lambda i: abs(batch_results[i]['cost'] - avg_cost))
            best_batch_result = batch_results[best_result_idx]
            
            # 전체 상태 업데이트
            all_executed_actions.extend(best_batch_result['actions'])
            current_budget -= best_batch_result['cost']
            
            for model_key, count in best_batch_result['inventory'].items():
                purchased_in_batch = count - current_inventory.get(model_key, 0)
                current_inventory[model_key] = count
                
                if model_key in remaining_targets:
                    remaining_targets[model_key] = max(0, remaining_targets[model_key] - purchased_in_batch)
            
            processed_auctions += len(current_batch)
            
            logger.info(f"배치 {batch_idx + 1} 완료: {len(best_batch_result['actions'])}개 낙찰, "
                       f"비용: {best_batch_result['cost']:,}원, 남은 예산: {current_budget:,}원")
            
            if current_inventory:
                inventory_summary = ", ".join(
                    f"{model_key}: {count}대" for model_key, count in current_inventory.items()
                )
                logger.info(f"누적 구매 현황: {inventory_summary}")

            if sum(remaining_targets.values()) <= 0 or current_budget <= 0:
                logger.info(f"조기 종료: 목표 달성 또는 예산 소진")
                break
        
        # 최종 결과 계산
        final_purchased = sum(current_inventory.values())
        final_cost = budget - current_budget
        
        total_targets = sum([plan['target_units'] for plan in purchase_plans])
        success_rate = final_purchased / total_targets if total_targets > 0 else 0
        
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        cache_hit_rate = self._cache_hits / max(self._cache_hits + self._cache_misses, 1) * 100
        
        message = textwrap.dedent(f"""
            [Rolling Horizon MCTS 최적화 완료]
            - 처리 시간: {processing_time:.1f}초
            - 전체 경매 수: {len(auction_schedule)}개 (모두 처리)
            - 처리된 배치 수: {min(total_batches, batch_idx + 1)}개
            - 총 구매 차량: {final_purchased}대
            - 총 사용 예산: {final_cost:,}원
            - 목표 달성률: {success_rate * 100:.1f}%
            - 예산 활용률: {(final_cost / budget) * 100:.1f}%
            - 캐시 적중률: {cache_hit_rate:.1f}%
            - 실행된 액션 수: {len(all_executed_actions)}개
        """)

        logger.info(message)

        if success_rate < 0.8:
            alert_message = textwrap.dedent(f"""
                ⚠️  목표 달성률이 {success_rate * 100:.1f}%로 낮습니다. 예산 증액이나 더 많은 경매 참여를 고려해보세요.
            """)
            logger.warning(alert_message)
            message += alert_message

        return {
            'message': message,
            'expected_purchase_units': final_purchased,
            'total_expected_cost': final_cost,
            'success_rate': round(success_rate * 100, 2),
            'budget_utilization': round((final_cost / budget) * 100, 2),
            'auction_list': all_executed_actions,
            'purchase_breakdown': current_inventory,
            'targets': remaining_targets,
            'processing_stats': {
                'processing_time_seconds': round(processing_time, 2),
                'total_auctions': len(auction_schedule),
                'processed_batches': min(total_batches, batch_idx + 1),
                'cache_hit_rate': round(cache_hit_rate, 2),
                'total_actions_executed': len(all_executed_actions),
            },
            'mcts_stats': {
                'total_iterations': iterations,
                'batches_processed': min(total_batches, batch_idx + 1),
                'max_tree_depth': self.config['max_tree_depth'],
                'lookahead_window': self.config['lookahead_window'],
                'rolling_horizon': True,
            }
        }

class ReoptimizationMCTSOptimizer(MCTSAuctionOptimizer):
    """재최적화를 위한 MCTS 옵티마이저"""
    
    def analyze_bid_performance(self, executed_bids: List, current_inventory: Dict[str, int]) -> Dict:
        """입찰 성과 분석"""
        if not executed_bids:
            return {"message": "실행된 입찰이 없습니다."}
            
        total_bids = len(executed_bids)
        won_bids = [bid for bid in executed_bids if bid.get('actual_result') == "won"]
        lost_bids = [bid for bid in executed_bids if bid.get('actual_result') == "lost"] 
        pending_bids = [bid for bid in executed_bids if bid.get('actual_result') == "pending"]
        
        win_rate = len(won_bids) / total_bids if total_bids > 0 else 0
        
        total_spent = sum(bid.get('actual_price', 0) for bid in won_bids)
        expected_total = sum(bid.get('expected_price', 0) for bid in executed_bids)
        cost_efficiency = total_spent / expected_total if expected_total > 0 else 0
        
        # 브랜드별 성과 분석
        brand_performance = defaultdict(lambda: {'won': 0, 'lost': 0, 'spent': 0})
        for bid in executed_bids:
            brand = bid.get('brand', 'unknown')
            if bid.get('actual_result') == "won":
                brand_performance[brand]['won'] += 1
                brand_performance[brand]['spent'] += bid.get('actual_price', 0)
            elif bid.get('actual_result') == "lost":
                brand_performance[brand]['lost'] += 1
        
        return {
            "total_bids": total_bids,
            "won_bids": len(won_bids),
            "lost_bids": len(lost_bids), 
            "pending_bids": len(pending_bids),
            "win_rate": round(win_rate * 100, 2),
            "total_spent": total_spent,
            "expected_total": expected_total,
            "cost_efficiency": round(cost_efficiency * 100, 2),
            "brand_performance": dict(brand_performance),
            "current_inventory": current_inventory,
            "won_vehicles": [
                {
                    "auction_house": bid.get('auction_house', 'unknown'),
                    "listing_id": bid.get('listing_id'),
                    "brand": bid.get('brand', 'unknown'),
                    "model": bid.get('model', 'any'),
                    "year": bid.get('year', 0),
                    "expected_price": bid.get('expected_price', 0),
                    "actual_price": bid.get('actual_price', 0),
                    "savings": bid.get('expected_price', 0) - bid.get('actual_price', 0)
                }
                for bid in won_bids
            ]
        }
    
    def reoptimize_strategy(self, reopt_input: Dict, iterations: int = 3000) -> Dict:
        """재최적화"""
        start_time = datetime.now()
        
        remaining_budget = reopt_input.get('remaining_budget', 0)
        remaining_auction_schedule = reopt_input.get('remaining_auction_schedule', [])
        executed_bids = reopt_input.get('executed_bids', [])
        current_inventory = reopt_input.get('current_inventory', {})
        remaining_targets = reopt_input.get('remaining_targets', {})
        
        logger.info(f"재최적화 시작 - 남은 예산: {remaining_budget:,}원, "
                   f"남은 경매: {len(remaining_auction_schedule)}개")
        
        performance_analysis = self.analyze_bid_performance(executed_bids, current_inventory)
        
        optimization_input = {
            "budget": remaining_budget,
            "purchase_plans": [
                {
                    "brand": key.split("_")[0] if "_" in key else key,
                    "model": key.split("_")[1] if len(key.split("_")) > 1 else None,
                    "year": int(key.split("_")[2]) if len(key.split("_")) > 2 else None,
                    "target_units": target_count
                }
                for key, target_count in remaining_targets.items()
                if target_count > 0
            ],
            "auction_schedule": remaining_auction_schedule
        }
        
        optimization_result = self.optimize_auction_strategy(optimization_input, iterations)
        
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        reoptimization_message = textwrap.dedent(f"""
            [재최적화 완료]
            - 처리 시간: {processing_time:.1f}초
            - 기존 입찰 성과: 승률 {performance_analysis.get('win_rate', 0):.1f}%
            - 추가 구매 예상: {optimization_result.get('expected_purchase_units', 0)}대
        """)
        
        logger.info(reoptimization_message)
        
        return {
            "message": reoptimization_message,
            "performance_analysis": performance_analysis,
            "reoptimization_result": optimization_result,
            "processing_stats": {
                "processing_time_seconds": round(processing_time, 2),
                "remaining_auctions": len(remaining_auction_schedule),
                "executed_bids": len(executed_bids),
            }
        }

# 사용 예시
if __name__ == "__main__":
    logger.info("\n" + "=" * 70)
    logger.info("MCTS 경매 최적화 시스템 테스트")
    logger.info("=" * 70)
    
    # 기본 샘플 데이터
    np.random.seed(42)
    random.seed(42)
    
    sample_data = []
    brands_models = [
        ('현대', '아반떼'), ('기아', 'K5'), ('현대', '소나타'), 
        ('기아', '스포티지'), ('현대', '투싼'), ('삼성', None), ('르노', None)
    ]
    
    for _ in range(1000):
        brand, model = random.choice(brands_models)
        year = random.choice([2020, 2021, 2022, 2023, None])
        mileage = random.randint(10000, 80000) if random.random() > 0.1 else None
        
        base_price = 20000000  # 기본 가격
        if model:
            model_prices = {
                '아반떼': 15000000, 'K5': 18000000, '소나타': 20000000,
                '스포티지': 25000000, '투싼': 23000000
            }
            base_price = model_prices.get(model, 20000000)
        
        age_discount = ((2024 - (year or 2022)) * 0.1) if year else 0.1
        mileage_discount = ((mileage or 50000) / 100000) * 0.15
        price = int(base_price * (1 - age_discount - mileage_discount) * random.uniform(0.85, 1.15))
        
        sample_data.append({
            'brand': brand,
            'model': model,
            'year': year,
            'mileage_km': mileage,
            'transmission': random.choice(['오토', '수동', None]),
            'fuel': random.choice(['가솔린', '디젤', '하이브리드', None]),
            'color': random.choice(['흰색', '검정', '은색', '회색', None]),
            'displacement_cc': random.choice([1600, 2000, 2400, None]),
            'auction_house': random.choice(['오토허브', '엔카오토', '케이카']),
            'winning_price': price,
            'auction_date': datetime.now() - timedelta(days=random.randint(1, 365))
        })
    
    history_data = pd.DataFrame(sample_data)
    
    # 최적화 시스템 초기화
    optimizer = MCTSAuctionOptimizer(history_data)
    
    # 최적화 입력 (일부 정보 누락)
    optimization_input = {
        'budget': 100000000,
        'purchase_plans': [
            {'brand': '현대', 'target_units': 5},
            {'brand': '기아', 'model': 'K5', 'target_units': 3},
            {'brand': '삼성', 'target_units': 2},
        ],
        'auction_schedule': [
            {
                'listing_id': 'auction001',
                'brand': '현대',
                'mileage_km': None,
                'auction_house': '오토허브',
                'min_price': 12000000,
                'date': '2025-09-01'
            },
            {
                'listing_id': 'auction002', 
                'brand': '현대',
                'model': '아반떼',
                'mileage_km': 25000,
                'auction_house': '엔카오토',
                'min_price': 13500000,
                'date': '2025-09-02'
            },
            {
                'listing_id': 'auction003',
                'brand': '기아',
                'model': 'K5',
                'year': 2022,
                'mileage_km': 32000,
                'auction_house': '케이카',
                'min_price': 15000000,
                'date': '2025-09-03'
            },
            {
                'listing_id': 'auction004',
                'brand': '삼성',
                'mileage_km': 40000,
                'auction_house': '오토허브',
                'min_price': 18000000,
                'date': '2025-09-04'
            }
        ]
    }
    
    logger.info("최적화 테스트 시작...")
    result = optimizer.optimize_auction_strategy(
        optimization_input=optimization_input,
        iterations=500
    )
    
    logger.info("\n최적화 결과:")
    print(json.dumps(result, indent=2, ensure_ascii=False))