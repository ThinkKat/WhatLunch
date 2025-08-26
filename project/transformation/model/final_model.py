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
        return (len(self.available_auctions) == 0 or 
                self.current_budget <= 0 or 
                sum(self.remaining_targets.values()) <= 0)
    
    def get_model_key(self, auction_item: AuctionItem) -> str:
        """경매 아이템으로부터 모델 키 생성"""
        return f"{auction_item.brand}_{auction_item.model}_{auction_item.year}"

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
        """현재 상태에서 가능한 액션들 초기화 - 과거 데이터 기반 입찰가 추천"""
        if self.state.is_terminal():
            return
            
        # 현재 진행 가능한 경매가 있다면
        if self.state.available_auctions:
            current_auction = self.state.available_auctions[0]  # 다음 경매
            model_key = self.state.get_model_key(current_auction)
            
            # 해당 모델이 구매 목표에 있고, 아직 목표 달성 안 했다면
            if (model_key in self.state.remaining_targets and 
                self.state.remaining_targets[model_key] > 0):
                
                # Skip 액션은 항상 추가
                self.untried_actions.append(
                    AuctionAction(current_auction.listing_id, 0, "skip")
                )
                
                # 과거 데이터에서 유사 차량의 실제 낙찰가들 가져오기
                historical_prices = self._get_historical_bid_prices(current_auction)
                
                if historical_prices:
                    # minimum_price보다 높은 가격들만 필터링
                    valid_prices = [p for p in historical_prices if p >= current_auction.min_price]
                    
                    if valid_prices:
                        # 예산 범위 내 가격들만 선택
                        affordable_prices = [p for p in valid_prices if p <= self.state.current_budget]
                        
                        if affordable_prices:
                            # 가격을 정렬해서 분위수별로 액션 생성
                            sorted_prices = sorted(affordable_prices)
                            n = len(sorted_prices)
                            
                            # 25%, 50%, 75%, 90% 분위수에 해당하는 가격들 선택
                            percentiles = [0.25, 0.5, 0.75, 0.9]
                            action_types = ["conservative", "moderate", "aggressive", "very_aggressive"]
                            
                            used_prices = set()
                            for percentile, action_type in zip(percentiles, action_types):
                                idx = min(int(n * percentile), n - 1)
                                bid_price = sorted_prices[idx]
                                
                                # 중복 가격 방지
                                if bid_price not in used_prices and bid_price >= current_auction.min_price:
                                    self.untried_actions.append(
                                        AuctionAction(current_auction.listing_id, bid_price, action_type)
                                    )
                                    used_prices.add(bid_price)
                                    
                        else:
                            # 예산 부족하면 최소가만 시도
                            if current_auction.min_price <= self.state.current_budget:
                                self.untried_actions.append(
                                    AuctionAction(current_auction.listing_id, current_auction.min_price, "min_price")
                                )
                else:
                    # 과거 데이터가 없는 경우 fallback (최소가 기반)
                    min_bid = current_auction.min_price
                    max_affordable = min(self.state.current_budget, min_bid * 1.5)
                    
                    if max_affordable >= min_bid:
                        fallback_multipliers = [1.05, 1.15, 1.25]
                        fallback_types = ["conservative", "moderate", "aggressive"]
                        
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
        """
        과거 유사 차량의 실제 낙찰가 데이터 가져오기
        """
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
                
                # 시간 가중치 계산
                time_weights = self.optimizer._calculate_time_weights(date_list)
                
                # 가중치가 높은 (최근의) 가격들을 더 많이 샘플링
                for price, weight in zip(price_list, time_weights):
                    # 가중치에 비례해서 해당 가격을 여러 번 추가 (최대 5번)
                    repeat_count = max(1, int(weight * 5))
                    historical_prices.extend([int(price)] * repeat_count)
            
            # 중복 제거하고 정렬
            unique_prices = sorted(list(set(historical_prices)))
            return unique_prices
            
        except Exception as e:
            print(f"과거 가격 데이터 가져오기 실패: {e}")
            return []
    
    def is_fully_expanded(self) -> bool:
        """모든 가능한 액션이 확장되었는지"""
        return len(self.untried_actions) == 0
    
    def uct_value(self, c=1.414) -> float:
        """UCT 값 계산"""
        if self.visits == 0:
            return float('inf')
        
        exploitation = self.total_reward / self.visits
        exploration = c * math.sqrt(math.log(self.parent.visits) / self.visits)
        return exploitation + exploration
    
    def best_child(self, c=1.414):
        """UCT 기준으로 최고의 자식 노드 선택"""
        if not self.children:
            raise ValueError(f"Node has no children. State: terminal={self.state.is_terminal()}, visits={self.visits}")
        return max(self.children, key=lambda child: child.uct_value(c))
    
    def add_child(self, action: AuctionAction, state: AuctionState):
        """자식 노드 추가"""
        # 부모의 optimizer 참조를 자식에게 전달
        optimizer_ref = self.optimizer if self.optimizer else (self.parent.optimizer if self.parent else None)
        child = MCTSNode(state, action, self, optimizer_ref)
        self.children.append(child)
        return child

class MCTSAuctionOptimizer:
    def __init__(self, historical_data: pd.DataFrame):
        """
        MCTS 기반 중고차 경매 최적화 시스템
        
        Args:
            historical_data: 역사적 경매 데이터 (기존 CarAuctionOptimizer와 동일한 형태)
        """
        self.historical_data = historical_data
        self.current_date = datetime.now()
        self._preprocess_data()
        
    def _preprocess_data(self):
        """기존 CarAuctionOptimizer와 동일한 전처리"""
        columns_for_key = [col for col in self.historical_data.columns if col not in ['winning_price', 'auction_date']]

        self.historical_data["auction_date"] = pd.to_datetime(self.historical_data["auction_date"], format='mixed')

        # 파생변수 생성
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
        """시간 가중치 계산 (기존과 동일)"""
        weights = []
        for date in dates:
            days_diff = (self.current_date - date).days
            time_weight = math.exp(-days_diff / 180)
            weights.append(min(time_weight, 1.0))
        return weights
    
    def _find_similar_cars(self, auction_item: AuctionItem) -> List[int]:
        """유사한 차량 인덱스 찾기"""
        filtered_df = self.processed_data[
            (self.processed_data['brand'] == auction_item.brand) & 
            (self.processed_data['model'] == auction_item.model)
        ]
        
        if len(filtered_df) == 0:
            return []
            
        similar_indices = []
        for idx, row in filtered_df.iterrows():
            # 연식 차이 ±2년, 주행거리 차이 ±30,000km 이내
            year_diff = abs(auction_item.year - row['year'])
            mileage_diff = abs(auction_item.mileage_km - row['mileage_km'])
            
            if year_diff <= 2 and mileage_diff <= 30000:
                original_idx = self.processed_data.index.get_loc(idx)
                similar_indices.append(original_idx)
                
        return similar_indices
    
    def _calculate_win_probability(self, auction_item: AuctionItem, bid_price: float) -> float:
        """입찰 성공 확률 계산"""
        similar_indices = self._find_similar_cars(auction_item)
        
        if not similar_indices:
            # 유사 차량이 없는 경우 보수적 확률
            if bid_price >= auction_item.min_price * 1.2:
                return 0.6
            elif bid_price >= auction_item.min_price * 1.1:
                return 0.4
            else:
                return 0.2
        
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
            
            return weighted_success / total_weight if total_weight > 0 else 0.3
        
        return 0.3
    
    def _apply_action(self, state: AuctionState, action: AuctionAction) -> Tuple[AuctionState, float]:
        """액션을 적용해서 새로운 상태 생성"""
        new_state = deepcopy(state)
        reward = 0
        
        if not new_state.available_auctions:
            return new_state, reward
        
        current_auction = new_state.available_auctions[0]
        model_key = new_state.get_model_key(current_auction)
        
        # 경매 리스트에서 현재 경매 제거
        new_state.available_auctions = new_state.available_auctions[1:]
        new_state.time_step += 1
        
        if action.bid_amount == 0:  # Skip
            # 건너뛰기 - 상태 변화 없음
            pass
        else:
            # 입찰 시도
            win_prob = self._calculate_win_probability(current_auction, action.bid_amount)
            
            if random.random() < win_prob:  # 낙찰 성공
                # 실제 낙찰가는 입찰가의 85%~100% 사이
                actual_price = int(action.bid_amount * random.uniform(0.85, 1.0))
                actual_price = max(actual_price, current_auction.min_price)
                
                # 상태 업데이트
                new_state.current_budget -= actual_price
                new_state.current_inventory[model_key] = new_state.current_inventory.get(model_key, 0) + 1
                
                if model_key in new_state.remaining_targets:
                    new_state.remaining_targets[model_key] = max(0, new_state.remaining_targets[model_key] - 1)
                
                # 보상 계산: 목표 달성 + 가격 효율성
                target_achievement = 1.0  # 목표 달성했으므로 높은 보상
                price_efficiency = max(0, (action.bid_amount - actual_price) / action.bid_amount)
                reward = target_achievement + price_efficiency * 0.5
            else:
                # 낙찰 실패 - 약간의 페널티
                reward = -0.1
        
        return new_state, reward
    
    def _simulate(self, state: AuctionState) -> float:
        """시뮬레이션 (랜덤 플레이아웃)"""
        current_state = deepcopy(state)
        total_reward = 0
        
        while not current_state.is_terminal() and len(current_state.available_auctions) > 0:
            current_auction = current_state.available_auctions[0]
            model_key = current_state.get_model_key(current_auction)
            
            # 간단한 휴리스틱으로 액션 선택
            if (model_key in current_state.remaining_targets and 
                current_state.remaining_targets[model_key] > 0 and
                current_state.current_budget >= current_auction.min_price):
                
                # 랜덤하게 입찰가 결정
                max_bid = min(current_state.current_budget, int(current_auction.min_price * random.uniform(1.0, 1.3)))
                action = AuctionAction(current_auction.listing_id, max_bid, "random")
            else:
                # 건너뛰기
                action = AuctionAction(current_auction.listing_id, 0, "skip")
            
            current_state, reward = self._apply_action(current_state, action)
            total_reward += reward
        
        # 최종 보상: 목표 달성률 + 예산 효율성
        total_targets = sum(state.remaining_targets.values())
        achieved_targets = sum(current_state.current_inventory.values())
        
        if total_targets > 0:
            achievement_rate = achieved_targets / (total_targets + achieved_targets)
        else:
            achievement_rate = 1.0
            
        budget_efficiency = current_state.current_budget / state.current_budget
        
        final_reward = achievement_rate * 2.0 + budget_efficiency * 0.5
        return total_reward + final_reward
    
    def mcts_search(self, initial_state: AuctionState, iterations: int = 1000) -> MCTSNode:
        """MCTS 검색 실행"""
        root = MCTSNode(initial_state, optimizer=self)  # optimizer 참조 전달
        
        pbar = tqdm(range(iterations), desc="MCTS 진행", ncols=100)
        for i in pbar:
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

            # === tqdm 업데이트 부분 ===
            if (i + 1) % 50 == 0 or (i + 1) == iterations:
                if root.visits > 0:
                    avg_reward = root.total_reward / root.visits
                else:
                    avg_reward = 0
                best_child = max(root.children, key=lambda c: c.visits) if root.children else None
                best_action = f"{best_child.action.action_type}({best_child.action.bid_amount:,})" if best_child else "N/A"
                
                pbar.set_postfix({
                    "visits": root.visits,
                    "children": len(root.children),
                    "avgR": f"{avg_reward:.2f}",
                    "best": best_action
                })

        pbar.close()

        print(f"\n[MCTS 검색 완료]")
        print(f"Root visits: {root.visits}, children: {len(root.children)}")
        if root.children:
            best_child = max(root.children, key=lambda c: c.visits)
            bid_info = f"bid={best_child.action.bid_amount:,}" if best_child.action.bid_amount > 0 else "skip"
            print(f"Best child: {best_child.action.action_type} ({bid_info}) - visits: {best_child.visits}")
        
        return root
    
    def get_best_action_sequence(self, root: MCTSNode, max_depth: int = 10) -> List[AuctionAction]:
        """최적 액션 시퀀스 추출"""
        sequence = []
        node = root
        depth = 0
        
        while node.children and depth < max_depth:
            # 가장 많이 방문된 자식 노드 선택 (exploitation)
            best_child = max(node.children, key=lambda child: child.visits)
            if best_child.action:  # action이 None이 아닌지 확인
                sequence.append(best_child.action)
            node = best_child
            depth += 1
        
        return sequence
    
    def optimize_auction_strategy(self, optimization_input: Dict, iterations: int = 1000) -> Dict:
        """MCTS를 사용한 경매 전략 최적화"""
        
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
                brand=item['brand'],
                model=item['model'],
                year=item['year'],
                mileage_km=item['mileage_km'],
                auction_house=item['auction_house'],
                min_price=item['min_price'],
                date=item['date'],
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
        
        # 결과 시뮬레이션
        final_state = deepcopy(initial_state)
        executed_actions = []
        total_cost = 0
        
        for action in best_actions:
            if final_state.is_terminal() or not final_state.available_auctions:
                break
                
            current_auction = final_state.available_auctions[0]
            
            # 액션 실행 (확률적)
            if action.bid_amount > 0:
                win_prob = self._calculate_win_probability(current_auction, action.bid_amount)
                if random.random() < win_prob:
                    actual_price = int(action.bid_amount * random.uniform(0.85, 1.0))
                    actual_price = max(actual_price, current_auction.min_price)
                    
                    executed_actions.append({
                        'auction_house': current_auction.auction_house,
                        'listing_id': current_auction.listing_id,
                        'max_bid_price': action.bid_amount,
                        'expected_price': actual_price,
                        'auction_end_date': current_auction.date,
                        'action_type': action.action_type,
                        'win_probability': win_prob
                    })
                    
                    total_cost += actual_price
            
            final_state, _ = self._apply_action(final_state, action)
        
        # 성과 계산
        total_purchased = sum(final_state.current_inventory.values())
        total_targets = sum(remaining_targets.values())
        success_rate = total_purchased / total_targets if total_targets > 0 else 0
        
        print(f"\n[MCTS 최적화 완료]")
        print(f"루트 노드 방문 횟수: {root.visits}")
        print(f"루트 노드 평균 보상: {root.total_reward / root.visits:.3f}")
        print(f"최적 액션 시퀀스 길이: {len(best_actions)}")
        
        return {
            'expected_purchase_units': total_purchased,
            'total_expected_cost': total_cost,
            'success_rate': round(success_rate * 100, 2),
            'budget_utilization': round((total_cost / budget) * 100, 2),
            'auction_list': executed_actions,
            'purchase_breakdown': dict(final_state.current_inventory),
            'mcts_stats': {
                'root_visits': root.visits,
                'root_avg_reward': root.total_reward / root.visits,
                'best_sequence_length': len(best_actions),
                'total_iterations': iterations
            }
        }

# 사용 예시
if __name__ == "__main__":
    # 샘플 데이터 생성
    np.random.seed(42)
    random.seed(42)

    hisotory_data = pd.read_csv("auction_results.csv")
    
    # MCTS 최적화 시스템 초기화
    optimizer = MCTSAuctionOptimizer(hisotory_data)
    
    # 최적화 입력 예시
    optimization_input = {
        'month': '2025-08-25',
        'budget': 50000000,  # 5천만원
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
            }
        ]
    }
    
    # MCTS 최적화 실행
    print("=" * 60)
    print("MCTS 기반 중고차 경매 최적화 시작")
    print("=" * 60)
    
    result = optimizer.optimize_auction_strategy(
        optimization_input=optimization_input,
        iterations=500  # 빠른 테스트를 위해 500회
    )
    
    print("\n" + "=" * 60)
    print("최적화 결과")
    print("=" * 60)
    print(json.dumps(result, indent=2, ensure_ascii=False))