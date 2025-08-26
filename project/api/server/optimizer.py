import pandas as pd
import numpy as np
import math
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
import random
import json
from collections import defaultdict
from copy import deepcopy
from tqdm import tqdm
import textwrap

@dataclass
class AuctionItem:
    listing_id: str
    brand: str
    model: str
    year: int
    mileage_km: int
    auction_house: str
    min_price: int
    date: str
    transmission: str = None
    fuel: str = None
    color: str = None
    displacement_cc: int = None

@dataclass
class AuctionState:
    """MCTS 노드의 상태를 나타내는 클래스"""
    current_budget: int
    remaining_targets: Dict[str, int]  # 모델별 남은 구매 목표 {"현대_아반떼_2023": 50}
    available_auctions: List[AuctionItem]  # 남은 경매들
    current_inventory: Dict[str, int]  # 현재까지 구매한 수량
    time_step: int  # 현재 시점 (몇 번째 의사결정인지)
    
    def is_terminal(self) -> bool:
        """터미널 상태인지 확인 (더 이상 진행할 수 없는 상태)"""
        # 목표를 모두 달성했거나, 경매가 없거나, 예산이 없는 경우
        targets_achieved = sum(self.remaining_targets.values()) <= 0
        no_auctions = len(self.available_auctions) == 0
        no_budget = self.current_budget <= 0
        
        return targets_achieved or no_auctions or no_budget
    
    def get_model_key(self, auction_item: AuctionItem) -> str:
        """경매 아이템으로부터 모델 키 생성"""
        return f"{auction_item.brand}_{auction_item.model}_{auction_item.year}"
    
    def get_target_achievement_rate(self) -> float:
        """목표 달성률 계산"""
        total_targets = sum(self.current_inventory.values()) + sum(self.remaining_targets.values())
        achieved = sum(self.current_inventory.values())
        return achieved / total_targets if total_targets > 0 else 1.0

@dataclass
class AuctionAction:
    """MCTS 액션을 나타내는 클래스"""
    auction_id: str
    bid_amount: int  # 0이면 건너뛰기
    action_type: str  # "skip", "conservative", "moderate", "aggressive"

class MCTSNode:
    """MCTS 트리의 노드"""
    def __init__(self, state: AuctionState, action: AuctionAction = None, parent=None, optimizer=None):
        self.state = state
        self.action = action  # 이 노드에 도달한 액션
        self.parent = parent
        self.optimizer = optimizer
        self.children = []
        self.visits = 0
        self.total_reward = 0.0
        self.untried_actions = []
        self._initialize_actions()
    
    def _initialize_actions(self):
        """현재 상태에서 가능한 액션들 초기화 - 목표 달성을 최우선으로"""
        if self.state.is_terminal():
            return
            
        if self.state.available_auctions:
            current_auction = self.state.available_auctions[0]
            model_key = self.state.get_model_key(current_auction)
            
            # 해당 모델이 구매 목표에 있고, 아직 목표 달성 안 했다면
            if (model_key in self.state.remaining_targets and 
                self.state.remaining_targets[model_key] > 0):
                
                # 과거 데이터에서 유사 차량의 실제 낙찰가들 가져오기
                historical_prices = self._get_historical_bid_prices(current_auction)
                
                if historical_prices:
                    valid_prices = [p for p in historical_prices if p >= current_auction.min_price]
                    
                    if valid_prices:
                        affordable_prices = [p for p in valid_prices if p <= self.state.current_budget]
                        
                        if affordable_prices:
                            sorted_prices = sorted(affordable_prices)
                            n = len(sorted_prices)
                            
                            # 목표 달성을 위해 더 공격적인 입찰 전략 추가
                            remaining_total = sum(self.state.remaining_targets.values())
                            remaining_auctions = len([a for a in self.state.available_auctions 
                                                    if self.state.get_model_key(a) in self.state.remaining_targets 
                                                    and self.state.remaining_targets[self.state.get_model_key(a)] > 0])
                            
                            urgency_factor = remaining_total / max(remaining_auctions, 1)
                            
                            if urgency_factor >= 1.0:  # 매우 긴급한 상황
                                percentiles = [0.4, 0.6, 0.8, 0.95]  # 더 공격적
                                action_types = ["moderate", "aggressive", "very_aggressive", "desperate"]
                            else:  # 일반적인 상황
                                percentiles = [0.25, 0.5, 0.75, 0.9]
                                action_types = ["conservative", "moderate", "aggressive", "very_aggressive"]
                            
                            used_prices = set()
                            for percentile, action_type in zip(percentiles, action_types):
                                idx = min(int(n * percentile), n - 1)
                                bid_price = sorted_prices[idx]
                                
                                if bid_price not in used_prices and bid_price >= current_auction.min_price:
                                    self.untried_actions.append(
                                        AuctionAction(current_auction.listing_id, bid_price, action_type)
                                    )
                                    used_prices.add(bid_price)
                                    
                            # 목표 달성이 어려운 상황에서는 skip 액션의 우선순위를 낮춤
                            if urgency_factor < 0.8:  # 여유가 있을 때만 skip 추가
                                self.untried_actions.append(
                                    AuctionAction(current_auction.listing_id, 0, "skip")
                                )
                        else:
                            # 예산 부족하지만 목표 달성이 중요한 경우
                            if current_auction.min_price <= self.state.current_budget:
                                self.untried_actions.append(
                                    AuctionAction(current_auction.listing_id, current_auction.min_price, "min_price")
                                )
                else:
                    # 과거 데이터가 없는 경우 - 목표 달성을 위해 더 적극적
                    min_bid = current_auction.min_price
                    max_affordable = min(self.state.current_budget, min_bid * 2.0)  # 더 넉넉한 범위
                    
                    if max_affordable >= min_bid:
                        fallback_multipliers = [1.1, 1.3, 1.5, 1.8]  # 더 공격적인 배수
                        fallback_types = ["conservative", "moderate", "aggressive", "very_aggressive"]
                        
                        for multiplier, action_type in zip(fallback_multipliers, fallback_types):
                            bid_price = int(min_bid * multiplier)
                            if bid_price <= max_affordable:
                                self.untried_actions.append(
                                    AuctionAction(current_auction.listing_id, bid_price, action_type)
                                )
            else:
                # 구매 목표가 없는 경매는 건너뛰기만
                self.untried_actions.append(
                    AuctionAction(current_auction.listing_id, 0, "skip")
                )
    
    def _get_historical_bid_prices(self, auction_item: AuctionItem) -> List[int]:
        """과거 유사 차량의 실제 낙찰가 데이터 가져오기"""
        if not self.optimizer:
            return []
            
        try:
            similar_indices = self.optimizer._find_similar_cars(auction_item)
            
            if not similar_indices:
                return []
            
            historical_prices = []
            for idx in similar_indices:
                car_info = self.optimizer.processed_data.iloc[idx]
                price_list = car_info['price_list']
                date_list = car_info['date_list']
                
                time_weights = self.optimizer._calculate_time_weights(date_list)
                
                for price, weight in zip(price_list, time_weights):
                    repeat_count = max(1, int(weight * 5))
                    historical_prices.extend([int(price)] * repeat_count)
            
            unique_prices = sorted(list(set(historical_prices)))
            return unique_prices
            
        except Exception as e:
            print(f"과거 가격 데이터 가져오기 실패: {e}")
            return []
    
    def is_fully_expanded(self) -> bool:
        """모든 가능한 액션이 확장되었는지"""
        return len(self.untried_actions) == 0
    
    def uct_value(self, c=1.414) -> float:
        """UCT 값 계산 - 목표 달성률을 고려한 보정"""
        if self.visits == 0:
            return float('inf')
        
        exploitation = self.total_reward / self.visits
        exploration = c * math.sqrt(math.log(self.parent.visits) / self.visits)
        
        # 목표 달성이 어려운 상황에서는 exploration을 더 강화
        achievement_rate = self.state.get_target_achievement_rate()
        if achievement_rate < 0.5:  # 목표 달성률이 50% 미만이면
            exploration *= 1.5  # exploration 가중치 증가
        
        return exploitation + exploration
    
    def best_child(self, c=1.414):
        """UCT 기준으로 최고의 자식 노드 선택"""
        if not self.children:
            raise ValueError(f"Node has no children. State: terminal={self.state.is_terminal()}, visits={self.visits}")
        return max(self.children, key=lambda child: child.uct_value(c))
    
    def add_child(self, action: AuctionAction, state: AuctionState):
        """자식 노드 추가"""
        optimizer_ref = self.optimizer if self.optimizer else (self.parent.optimizer if self.parent else None)
        child = MCTSNode(state, action, self, optimizer_ref)
        self.children.append(child)
        return child

class MCTSAuctionOptimizer:
    def __init__(self, historical_data: pd.DataFrame):
        """MCTS 기반 중고차 경매 최적화 시스템"""
        self.historical_data = historical_data
        self.current_date = datetime.now()
        self._preprocess_data()
        
    def _preprocess_data(self):
        """기존 CarAuctionOptimizer와 동일한 전처리"""
        columns_for_key = [col for col in self.historical_data.columns if col not in ['winning_price', 'auction_date']]

        self.historical_data["auction_date"] = pd.to_datetime(self.historical_data["auction_date"], format='mixed')
        self.historical_data["age"] = self.historical_data["auction_date"].dt.year - self.historical_data["year"] 

        def q50(x): return x.quantile(0.5)
        def q75(x): return x.quantile(0.75)
        def q90(x): return x.quantile(0.9)
        
        self.processed_data = (
            self.historical_data.groupby(columns_for_key, dropna=False)
            .agg(
                reliability=("winning_price","count"),
                min=("winning_price","min"),
                max=("winning_price","max"),
                mean=("winning_price","mean"),
                median=("winning_price","median"),
                std=("winning_price","std"),
                q50=("winning_price", q50),
                q75=("winning_price", q75),
                q90=("winning_price", q90),
                price_list=("winning_price", lambda x: list(x)),
                date_list=("auction_date",  lambda x: list(x)),
            )
            .reset_index()
        )

        self.processed_data["price_range"] = self.processed_data["max"] - self.processed_data["min"]
        print(f"[전처리 완료] {len(self.processed_data)}개 차량 데이터")
    
    def _calculate_time_weights(self, dates: List[datetime]) -> List[float]:
        """시간 가중치 계산"""
        weights = []
        for date in dates:
            days_diff = (self.current_date - date).days
            time_weight = math.exp(-days_diff / 180)
            weights.append(min(time_weight, 1.0))
        return weights
    
    def _find_similar_cars(self, auction_item: AuctionItem) -> List[int]:
        """유사한 차량 인덱스 찾기 - 계층적 유사도 검색"""
        
        # 1차: 정확한 브랜드+모델 매칭
        exact_match_df = self.processed_data[
            (self.processed_data['brand'] == auction_item.brand) & 
            (self.processed_data['model'] == auction_item.model)
        ]
        
        if len(exact_match_df) > 0:
            similar_indices = []
            for idx, row in exact_match_df.iterrows():
                year_diff = abs(auction_item.year - row['year'])
                mileage_diff = abs(auction_item.mileage_km - row['mileage_km'])
                
                if year_diff <= 2 and mileage_diff <= 30000:
                    original_idx = self.processed_data.index.get_loc(idx)
                    similar_indices.append(original_idx)
            
            if similar_indices:
                return similar_indices
        
        # 2차: 같은 브랜드 내 다른 모델 (유사한 연식/주행거리)
        brand_match_df = self.processed_data[
            self.processed_data['brand'] == auction_item.brand
        ]
        
        if len(brand_match_df) > 0:
            similar_indices = []
            for idx, row in brand_match_df.iterrows():
                year_diff = abs(auction_item.year - row['year'])
                mileage_diff = abs(auction_item.mileage_km - row['mileage_km'])
                
                # 다른 모델이므로 더 엄격한 조건
                if year_diff <= 1 and mileage_diff <= 20000:
                    original_idx = self.processed_data.index.get_loc(idx)
                    similar_indices.append(original_idx)
            
            if similar_indices:
                print(f"⚠️  {auction_item.brand} {auction_item.model}의 정확한 매칭이 없어 동일 브랜드 {len(similar_indices)}개 차량을 참조합니다.")
                return similar_indices
        
        # 3차: 전체 데이터에서 유사한 연식의 차량 (최후의 수단)
        fallback_df = self.processed_data[
            abs(self.processed_data['year'] - auction_item.year) <= 1
        ]
        
        if len(fallback_df) > 0:
            # 무작위로 최대 10개 샘플 선택
            sample_size = min(10, len(fallback_df))
            sampled_indices = fallback_df.sample(n=sample_size).index.tolist()
            print(f"⚠️  {auction_item.brand} {auction_item.model}의 유사 데이터가 없어 동일 연식 {sample_size}개 차량을 참조합니다.")
            return [self.processed_data.index.get_loc(idx) for idx in sampled_indices]
        
        print(f"⚠️  {auction_item.brand} {auction_item.model}의 참조 데이터가 전혀 없습니다. 기본 전략을 사용합니다.")
        return []
    
    def _calculate_win_probability(self, auction_item: AuctionItem, bid_price: float) -> float:
        """입찰 성공 확률 계산"""
        similar_indices = self._find_similar_cars(auction_item)
        
        if not similar_indices:
            # 참조 데이터가 전혀 없는 경우: 보수적이지만 합리적인 확률 모델
            price_ratio = bid_price / auction_item.min_price
            
            # 시장 일반론 기반 확률 (경험적 모델)
            if price_ratio >= 1.6:      # 최소가의 160% 이상
                return 0.85
            elif price_ratio >= 1.4:    # 140% 이상  
                return 0.70
            elif price_ratio >= 1.25:   # 125% 이상
                return 0.55
            elif price_ratio >= 1.15:   # 115% 이상
                return 0.40
            elif price_ratio >= 1.05:   # 105% 이상
                return 0.25
            else:                       # 최소가 근처
                return 0.15
        
        # 유사 차량들의 가격 데이터로 확률 계산
        all_prices = []
        all_weights = []
        
        for idx in similar_indices:
            car_info = self.processed_data.iloc[idx]
            price_list = car_info['price_list']
            date_list = car_info['date_list']
            time_weights = self._calculate_time_weights(date_list)
            
            all_prices.extend(price_list)
            all_weights.extend(time_weights)
        
        if all_prices:
            weighted_success = 0
            total_weight = 0
            
            for price, weight in zip(all_prices, all_weights):
                total_weight += weight
                if price <= bid_price:
                    weighted_success += weight
            
            calculated_prob = weighted_success / total_weight if total_weight > 0 else 0.3
            return min(calculated_prob, 0.95)  # 최대 95% 제한
        
        # fallback
        return 0.3
    
    def _apply_action(self, state: AuctionState, action: AuctionAction) -> Tuple[AuctionState, float]:
        """
        액션을 적용해서 새로운 상태 생성 - 개별 액션에는 보상 없음 (에피소드 종료시에만 보상)
        """
        new_state = deepcopy(state)
        reward = 0  # 개별 액션에는 보상 없음! 오직 에피소드 종료시에만
        
        if not new_state.available_auctions:
            return new_state, reward
        
        current_auction = new_state.available_auctions[0]
        model_key = new_state.get_model_key(current_auction)
        
        # 경매 리스트에서 현재 경매 제거
        new_state.available_auctions = new_state.available_auctions[1:]
        new_state.time_step += 1
        
        if action.bid_amount == 0:  # Skip
            # 건너뛰기 - 상태만 변경, 보상 없음
            pass
                
        else:
            # 입찰 시도
            win_prob = self._calculate_win_probability(current_auction, action.bid_amount)
            
            if random.random() < win_prob:  # 낙찰 성공
                actual_price = int(action.bid_amount * random.uniform(0.85, 1.0))
                actual_price = max(actual_price, current_auction.min_price)
                
                # 상태 업데이트만 - 보상은 에피소드 끝에서
                new_state.current_budget -= actual_price
                new_state.current_inventory[model_key] = new_state.current_inventory.get(model_key, 0) + 1
                
                if model_key in new_state.remaining_targets:
                    new_state.remaining_targets[model_key] = max(0, new_state.remaining_targets[model_key] - 1)
            # else: 낙찰 실패 - 상태 변화 없음, 보상도 없음
        
        return new_state, reward
    
    def _simulate(self, state: AuctionState) -> float:
        """
        시뮬레이션 - 최종 구매 대수만을 기준으로 보상 계산
        예산 대비 최대한 많은 차량 구매가 목표
        """
        current_state = deepcopy(state)
        
        while not current_state.is_terminal() and len(current_state.available_auctions) > 0:
            current_auction = current_state.available_auctions[0]
            model_key = current_state.get_model_key(current_auction)
            
            # 목표가 있는 차량에 대해서는 적극적으로 입찰
            if (model_key in current_state.remaining_targets and 
                current_state.remaining_targets[model_key] > 0 and
                current_state.current_budget >= current_auction.min_price):
                
                # 예산 활용도와 남은 경매 수를 고려한 입찰 전략
                remaining_budget_ratio = current_state.current_budget / state.current_budget
                remaining_relevant_auctions = len([a for a in current_state.available_auctions 
                                                 if current_state.get_model_key(a) in current_state.remaining_targets 
                                                 and current_state.remaining_targets[current_state.get_model_key(a)] > 0])
                
                # 예산이 많이 남았고 경매가 적다면 더 공격적으로
                if remaining_budget_ratio > 0.7 and remaining_relevant_auctions <= 3:
                    multiplier = random.uniform(1.3, 1.7)  # 공격적
                elif remaining_budget_ratio > 0.4:
                    multiplier = random.uniform(1.1, 1.4)  # 보통
                else:
                    multiplier = random.uniform(1.0, 1.2)  # 보수적
                
                max_bid = min(current_state.current_budget, 
                             int(current_auction.min_price * multiplier))
                action = AuctionAction(current_auction.listing_id, max_bid, "simulation")
            else:
                # 목표가 없거나 예산 부족이면 건너뛰기
                action = AuctionAction(current_auction.listing_id, 0, "skip")
            
            current_state, _ = self._apply_action(current_state, action)
        
        # ===== 핵심: 오직 최종 구매 대수만으로 보상 계산 =====
        total_purchased = sum(current_state.current_inventory.values())
        
        # 기본 보상: 구매한 차량 수에 비례
        base_reward = total_purchased * 10.0  # 차량 1대당 10점
        
        # 목표 달성 보너스
        total_targets = sum(state.remaining_targets.values())
        if total_targets > 0:
            achievement_rate = total_purchased / total_targets
            if achievement_rate >= 1.0:  # 목표 완전 달성
                achievement_bonus = 50.0
            elif achievement_rate >= 0.8:  # 80% 이상 달성
                achievement_bonus = 20.0
            elif achievement_rate >= 0.6:  # 60% 이상 달성
                achievement_bonus = 10.0
            else:
                achievement_bonus = 0
        else:
            achievement_bonus = 0
        
        # 예산 효율성 보너스 (남은 예산이 적을수록 좋음, 단 구매를 했을 때만)
        if total_purchased > 0:
            budget_utilization = 1.0 - (current_state.current_budget / state.current_budget)
            efficiency_bonus = budget_utilization * 5.0  # 최대 5점
        else:
            efficiency_bonus = 0
        
        final_reward = base_reward + achievement_bonus + efficiency_bonus
        
        return final_reward
    
    def mcts_search(self, initial_state: AuctionState, iterations: int = 1000) -> MCTSNode:
        """MCTS 검색 실행"""
        root = MCTSNode(initial_state, optimizer=self)

        print(f"[MCTS] Search start - iterations={iterations}")
        for i in range(iterations):
            # 1. Selection
            node = root
            while not node.state.is_terminal() and node.is_fully_expanded() and node.children:
                node = node.best_child()
            
            # 2. Expansion
            if not node.state.is_terminal() and not node.is_fully_expanded():
                if node.untried_actions:
                    action = random.choice(node.untried_actions)
                    node.untried_actions.remove(action)
                    new_state, _ = self._apply_action(node.state, action)
                    node = node.add_child(action, new_state)
            
            # 3. Simulation
            reward = self._simulate(node.state)
            
            # 4. Backpropagation
            while node is not None:
                node.visits += 1
                node.total_reward += reward
                node = node.parent

            # 로그
            if (i + 1) % 500 == 0 or (i + 1) == iterations:
                avg_reward = root.total_reward / root.visits if root.visits > 0 else 0
                best_child = max(root.children, key=lambda c: c.visits) if root.children else None
                best_action = (
                    f"{best_child.action.action_type}({best_child.action.bid_amount:,})"
                    if best_child else "N/A"
                )
                print(f"[MCTS] iter={i+1}/{iterations}, visits={root.visits}, "
                      f"children={len(root.children)}, avgR={avg_reward:.2f}, best={best_action}")

        print(f"[MCTS 검색 완료] Root visits={root.visits}, children={len(root.children)}")
        if root.children:
            best_child = max(root.children, key=lambda c: c.visits)
            bid_info = f"bid={best_child.action.bid_amount:,}" if best_child.action.bid_amount > 0 else "skip"
            print(f"[MCTS] Best child: {best_child.action.action_type} ({bid_info}), visits={best_child.visits}")

        return root
    
    def get_best_action_sequence(self, root: MCTSNode, max_depth: int = 10) -> List[AuctionAction]:
        """최적 액션 시퀀스 추출"""
        sequence = []
        node = root
        depth = 0
        
        while node.children and depth < max_depth:
            # 가장 많이 방문된 자식 노드 선택
            best_child = max(node.children, key=lambda child: child.visits)
            if best_child.action:
                sequence.append(best_child.action)
            node = best_child
            depth += 1
        
        return sequence
    
    def optimize_auction_strategy(self, optimization_input: Dict, iterations: int = 1000) -> Dict:
        """MCTS를 사용한 경매 전략 최적화 - 목표 달성 중심"""
        
        # 입력 데이터 파싱
        budget = optimization_input['budget']
        purchase_plans = optimization_input['purchase_plans']
        auction_schedule = optimization_input['auction_schedule']
        
        # 구매 목표 설정
        remaining_targets = {}
        for plan in purchase_plans:
            key = f"{plan['brand']}_{plan['model']}_{plan['year']}"
            remaining_targets[key] = plan['target_units']
        
        # 경매 아이템 생성
        available_auctions = []
        for item in auction_schedule:
            auction_item = AuctionItem(
                listing_id=item.get('listing_id', f"auction_{len(available_auctions)}"),
                brand=item.get('brand'),
                model=item.get('model'),
                year=item.get('year'),
                mileage_km=item.get('mileage_km'),
                auction_house=item.get('auction_house'),
                min_price=item.get('min_price'),
                date=item.get('date'),
                transmission=item.get('transmission'),
                fuel=item.get('fuel'),
                color=item.get('color'),
                displacement_cc=item.get('displacement_cc')
            )
            available_auctions.append(auction_item)
        
        # 초기 상태 생성
        initial_state = AuctionState(
            current_budget=budget,
            remaining_targets=remaining_targets,
            available_auctions=available_auctions,
            current_inventory={},
            time_step=0
        )
        
        print(f"MCTS 검색 시작 - {iterations}회 반복")
        print(f"초기 예산: {budget:,}원")
        print(f"구매 목표: {remaining_targets}")
        print(f"경매 일정: {len(available_auctions)}개")
        
        # MCTS 검색 실행
        root = self.mcts_search(initial_state, iterations)
        
        # 최적 액션 시퀀스 추출
        best_actions = self.get_best_action_sequence(root)
        
        # 결과를 여러 번 시뮬레이션해서 평균적인 성과 계산 (확률적 요소 때문)
        simulation_results = []
        for sim_run in range(100):  # 100번 시뮬레이션
            sim_state = deepcopy(initial_state)
            sim_executed_actions = []
            sim_total_cost = 0
            
            for action in best_actions:
                if sim_state.is_terminal() or not sim_state.available_auctions:
                    break
                    
                current_auction = sim_state.available_auctions[0]
                model_key = sim_state.get_model_key(current_auction)
                
                # 액션 실행
                if action.bid_amount > 0:
                    win_prob = self._calculate_win_probability(current_auction, action.bid_amount)
                    if random.random() < win_prob:
                        actual_price = int(action.bid_amount * random.uniform(0.85, 1.0))
                        actual_price = max(actual_price, current_auction.min_price)
                        
                        sim_executed_actions.append({
                            'auction_house': current_auction.auction_house,
                            'listing_id': current_auction.listing_id,
                            'max_bid_price': action.bid_amount,
                            'expected_price': actual_price,
                            'auction_end_date': current_auction.date,
                            'action_type': action.action_type,
                            'win_probability': win_prob
                        })
                        
                        sim_total_cost += actual_price
                
                sim_state, _ = self._apply_action(sim_state, action)
            
            sim_total_purchased = sum(sim_state.current_inventory.values())
            simulation_results.append({
                'purchased': sim_total_purchased,
                'cost': sim_total_cost,
                'actions': sim_executed_actions,
                'inventory': dict(sim_state.current_inventory)
            })
        
        # 시뮬레이션 결과 통계 계산
        avg_purchased = np.mean([r['purchased'] for r in simulation_results])
        avg_cost = np.mean([r['cost'] for r in simulation_results])
        
        # 가장 대표적인 시뮬레이션 결과 선택 (평균에 가장 가까운 것)
        best_sim_idx = min(range(len(simulation_results)), 
                          key=lambda i: abs(simulation_results[i]['purchased'] - avg_purchased))
        best_simulation = simulation_results[best_sim_idx]
        
        # 목표 달성률 계산
        total_targets = sum(remaining_targets.values())
        success_rate = avg_purchased / total_targets if total_targets > 0 else 0
        
        message = textwrap.dedent(f"""
            [MCTS 최적화 완료]
            - 루트 노드 방문 횟수: {root.visits}
            - 루트 노드 평균 보상: {root.total_reward / root.visits:.3f}
            - 최적 액션 시퀀스 길이: {len(best_actions)}
            - 100회 시뮬레이션 평균 구매: {avg_purchased:.1f}대
            - 목표 달성률: {success_rate * 100:.1f}%
        """)

        print(message)

        # 목표 달성률이 낮으면 경고
        if success_rate < 0.8:
            alert_message = textwrap.dedent(f"""
                ⚠️  목표 달성률이 {success_rate * 100:.1f}%로 낮습니다. 예산 증액을 고려해보세요
            """)
            print(alert_message)
            message += alert_message

        return {
            'message': message,
            'expected_purchase_units': int(avg_purchased),
            'total_expected_cost': int(avg_cost),
            'success_rate': round(success_rate * 100, 2),
            'budget_utilization': round((avg_cost / budget) * 100, 2),
            'auction_list': best_simulation['actions'],
            'purchase_breakdown': best_simulation['inventory'],
            'mcts_stats': {
                'root_visits': root.visits,
                'root_avg_reward': root.total_reward / root.visits if root.visits > 0 else 0,
                'best_sequence_length': len(best_actions),
                'total_iterations': iterations,
                'simulation_runs': 100
            }
        }

# 사용 예시 및 테스트
if __name__ == "__main__":
    # 향상된 테스트를 위한 샘플 데이터
    np.random.seed(42)
    random.seed(42)

    # 실제 CSV 파일 로드
    # history_data = pd.read_csv("auction_results.csv")
    
    # 테스트용 샘플 데이터 생성
    sample_data = []
    brands_models = [
        ('현대', '아반떼'), ('기아', 'K5'), ('현대', '소나타'), 
        ('기아', '스포티지'), ('현대', '투싼')
    ]
    
    for _ in range(3000):
        brand, model = random.choice(brands_models)
        year = random.choice([2020, 2021, 2022, 2023])
        mileage = random.randint(10000, 80000)
        
        base_price = {
            ('현대', '아반떼'): 15000000,
            ('기아', 'K5'): 18000000,
            ('현대', '소나타'): 20000000,
            ('기아', '스포티지'): 25000000,
            ('현대', '투싼'): 23000000
        }[brand, model]
        
        age_discount = (2024 - year) * 0.1
        mileage_discount = (mileage / 100000) * 0.15
        price = int(base_price * (1 - age_discount - mileage_discount) * random.uniform(0.85, 1.15))
        
        sample_data.append({
            'brand': brand,
            'model': model,
            'year': year,
            'mileage_km': mileage,
            'transmission': random.choice(['오토', '수동']),
            'fuel': random.choice(['가솔린', '디젤', '하이브리드']),
            'color': random.choice(['흰색', '검정', '은색', '회색']),
            'displacement_cc': random.choice([1600, 2000, 2400]),
            'auction_house': random.choice(['오토허브', '엔카오토', '케이카']),
            'winning_price': price,
            'auction_date': datetime.now() - timedelta(days=random.randint(1, 365))
        })
    
    history_data = pd.DataFrame(sample_data)
    
    # MCTS 최적화 시스템 초기화
    optimizer = MCTSAuctionOptimizer(history_data)

    optimization_input = {
        'month': '2025-08-25',
        'budget': 100000000,  # 1억원
        'purchase_plans': [
            {'brand': '현대', 'model': '아반떼', 'year': 2023, 'target_units': 3},
            {'brand': '기아', 'model': 'K5', 'year': 2022, 'target_units': 2}
        ],
        'auction_schedule': [
            {
                'listing_id': 'hub001',
                'brand': '현대', 'model': '아반떼', 'year': 2023,
                'mileage_km': 25000, 'auction_house': '오토허브',
                'min_price': 12000000, 'date': '2025-09-01',
                'transmission': '오토', 'fuel': '가솔린', 'color': '흰색'
            },
            {
                'listing_id': 'encar002',
                'brand': '현대', 'model': '아반떼', 'year': 2023,
                'mileage_km': 18000, 'auction_house': '엔카오토',
                'min_price': 13500000, 'date': '2025-09-02',
                'transmission': '오토', 'fuel': '가솔린', 'color': '검정'
            },
            {
                'listing_id': 'kcar003',
                'brand': '기아', 'model': 'K5', 'year': 2022,
                'mileage_km': 32000, 'auction_house': '케이카',
                'min_price': 15000000, 'date': '2025-09-03',
                'transmission': '오토', 'fuel': '가솔린', 'color': '은색'
            },
            {
                'listing_id': 'hub004',
                'brand': '현대', 'model': '아반떼', 'year': 2023,
                'mileage_km': 28000, 'auction_house': '오토허브',
                'min_price': 11800000, 'date': '2025-09-04',
                'transmission': '오토', 'fuel': '가솔린', 'color': '회색'
            },
            {
                'listing_id': 'encar005',
                'brand': '기아', 'model': 'K5', 'year': 2022,
                'mileage_km': 29000, 'auction_house': '엔카오토',
                'min_price': 14800000, 'date': '2025-09-05',
                'transmission': '오토', 'fuel': '디젤', 'color': '흰색'
            },
            {
                'listing_id': 'hub006',
                'brand': '현대', 'model': '아반떼', 'year': 2023,
                'mileage_km': 22000, 'auction_house': '오토허브',
                'min_price': 12500000, 'date': '2025-09-06',
                'transmission': '오토', 'fuel': '가솔린', 'color': '파랑'
            },
            {
                'listing_id': 'kcar007',
                'brand': '기아', 'model': 'K5', 'year': 2022,
                'mileage_km': 35000, 'auction_house': '케이카',
                'min_price': 14500000, 'date': '2025-09-07',
                'transmission': '오토', 'fuel': '가솔린', 'color': '회색'
            }
        ]
    }
    
    # MCTS 최적화 실행
    print("=" * 60)
    print("MCTS 기반 중고차 경매 최적화 시작")
    print("=" * 60)
    
    result = optimizer.optimize_auction_strategy(
        optimization_input=optimization_input,
        iterations=2000
    )
    
    print("\n" + "=" * 60)
    print("최적화 결과")
    print("=" * 60)
    print(json.dumps(result, indent=2, ensure_ascii=False))