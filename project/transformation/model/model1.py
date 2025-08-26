import pandas as pd
import numpy as np
import random
from itertools import combinations, product
from typing import List, Dict, Tuple, Optional
import ast
import warnings
warnings.filterwarnings('ignore')

class CarAuctionOptimizer:
    def __init__(self, df: pd.DataFrame):
        """
        중고차 경매 포트폴리오 최적화 시스템
        
        Args:
            df: 경매 데이터 (price_list 컬럼 포함)
        """
        self.df = df.copy()
        self._preprocess_data()
        
    def _preprocess_data(self):
        """데이터 전처리"""
        # price_list가 문자열인 경우 리스트로 변환
        if isinstance(self.df['price_list'].iloc[0], str):
            self.df['price_list'] = self.df['price_list'].apply(ast.literal_eval)
        
        # 가격 리스트가 비어있거나 None인 경우 제거
        self.df = self.df[self.df['price_list'].apply(lambda x: x and len(x) > 0)].reset_index(drop=True)
        
        # 신뢰도 계산 (가격 데이터 개수)
        self.df['reliability'] = self.df['price_list'].apply(len)
        
        print(f"전처리 완료: {len(self.df)}개 차량 데이터")
        
    def calculate_win_probability(self, car_idx: int, bid_price: float) -> float:
        """
        특정 차량에 대한 입찰 성공 확률 계산
        
        Args:
            car_idx: 차량 인덱스
            bid_price: 입찰가
            
        Returns:
            성공 확률 (0~1)
        """
        price_list = self.df.iloc[car_idx]['price_list']
        success_count = sum(1 for price in price_list if price <= bid_price)
        return success_count / len(price_list)
    
    def monte_carlo_simulation(self, 
                             portfolio: List[Tuple[int, float]], 
                             budget: float,
                             target_cars: int,
                             n_simulations: int = 10000) -> Dict:
        """
        몬테카르로 시뮬레이션으로 포트폴리오 성과 평가
        
        Args:
            portfolio: [(car_idx, bid_price), ...] 형태의 포트폴리오
            budget: 총 예산
            target_cars: 목표 차량 수
            n_simulations: 시뮬레이션 횟수
            
        Returns:
            시뮬레이션 결과
        """
        results = {
            'success_count': 0,  # 목표 달성 횟수
            'total_spent_list': [],
            'cars_won_list': [],
            'individual_success_rates': []
        }
        
        for simulation in range(n_simulations):
            total_spent = 0
            cars_won = 0
            simulation_results = []
            
            for car_idx, bid_price in portfolio:
                # 실제 낙찰가를 랜덤 샘플링
                price_list = self.df.iloc[car_idx]['price_list']
                actual_winning_price = random.choice(price_list)
                
                if bid_price >= actual_winning_price:
                    total_spent += actual_winning_price
                    cars_won += 1
                    simulation_results.append(True)
                else:
                    simulation_results.append(False)
                    
                # 예산 초과 시 중단
                if total_spent > budget:
                    break
            
            results['cars_won_list'].append(cars_won)
            results['total_spent_list'].append(min(total_spent, budget))
            
            # 목표 달성 여부 (목표 차량 수 달성 & 예산 내)
            if cars_won >= target_cars and total_spent <= budget:
                results['success_count'] += 1
        
        # 결과 통계 계산
        results['success_rate'] = results['success_count'] / n_simulations
        results['avg_cars_won'] = np.mean(results['cars_won_list'])
        results['avg_spent'] = np.mean(results['total_spent_list'])
        results['budget_utilization'] = results['avg_spent'] / budget
        
        return results
    
    def generate_bid_allocations(self, budget: float, num_cars: int, min_bid_ratio: float = 0.1) -> List[List[float]]:
        """
        예산을 차량별로 배분하는 여러 전략 생성
        
        Args:
            budget: 총 예산
            num_cars: 차량 수
            min_bid_ratio: 최소 입찰 비율
            
        Returns:
            입찰가 배분 리스트
        """
        allocations = []
        min_bid = budget * min_bid_ratio
        
        # 균등 분할
        equal_bid = budget / num_cars
        if equal_bid >= min_bid:
            allocations.append([equal_bid] * num_cars)
        
        # 비율 기반 분할 (여러 패턴)
        if num_cars == 2:
            ratios = [(0.3, 0.7), (0.4, 0.6), (0.5, 0.5), (0.6, 0.4), (0.7, 0.3)]
        elif num_cars == 3:
            ratios = [(0.3, 0.3, 0.4), (0.25, 0.35, 0.4), (0.33, 0.33, 0.34)]
        else:
            # 더 많은 차량의 경우 균등 분할만 사용
            return allocations
        
        for ratio in ratios:
            allocation = [budget * r for r in ratio]
            if all(bid >= min_bid for bid in allocation):
                allocations.append(allocation)
        
        return allocations
    
    def find_optimal_portfolio(self, 
                             budget: float,
                             target_cars: int,
                             max_candidates: int = 50,
                             min_reliability: int = 2) -> Dict:
        """
        최적 포트폴리오 찾기
        
        Args:
            budget: 총 예산
            target_cars: 목표 차량 수  
            max_candidates: 후보 차량 최대 수
            min_reliability: 최소 신뢰도 (가격 데이터 개수)
            
        Returns:
            최적 포트폴리오 정보
        """
        # 신뢰도 기반 필터링
        reliable_cars = self.df[self.df['reliability'] >= min_reliability].copy()
        
        if len(reliable_cars) == 0:
            raise ValueError("신뢰도 조건을 만족하는 차량이 없습니다.")
        
        # 가격 범위 기반 후보 선별 (예산의 80% 이하 평균 가격)
        max_avg_price = budget * 0.8 / target_cars
        candidates = reliable_cars[reliable_cars['mean'] <= max_avg_price].copy()
        
        if len(candidates) < target_cars:
            print(f"경고: 예산에 맞는 차량이 부족합니다. 기준을 완화합니다.")
            candidates = reliable_cars.nsmallest(max_candidates, 'mean')
        
        candidates = candidates.head(max_candidates).reset_index(drop=True)
        print(f"후보 차량 수: {len(candidates)}")
        
        best_portfolio = None
        best_success_rate = 0
        best_results = None
        
        # 가능한 차량 조합 탐색
        total_combinations = 0
        evaluated_combinations = 0
        
        for car_indices in combinations(range(len(candidates)), target_cars):
            # 각 조합에 대해 여러 입찰 전략 시도
            bid_allocations = self.generate_bid_allocations(budget, target_cars)
            
            for bid_allocation in bid_allocations:
                total_combinations += 1
                
                # 포트폴리오 구성
                portfolio = list(zip(car_indices, bid_allocation))
                
                # 기본적인 실현 가능성 체크
                total_min_price = sum(candidates.iloc[idx]['min'] for idx, _ in portfolio)
                if total_min_price > budget:
                    continue
                
                evaluated_combinations += 1
                
                # 몬테카르로 시뮬레이션으로 평가
                results = self.monte_carlo_simulation(
                    [(candidates.index[idx], bid) for idx, bid in portfolio], 
                    budget, target_cars, n_simulations=5000
                )
                
                if results['success_rate'] > best_success_rate:
                    best_success_rate = results['success_rate']
                    best_portfolio = portfolio
                    best_results = results
                
                # 진행상황 출력 (100개마다)
                if evaluated_combinations % 100 == 0:
                    print(f"평가 진행: {evaluated_combinations}/{total_combinations}, 현재 최고: {best_success_rate:.3f}")
        
        if best_portfolio is None:
            raise ValueError("실현 가능한 포트폴리오를 찾을 수 없습니다.")
        
        # 결과 포맷팅
        result = {
            'portfolio': [],
            'success_rate': best_success_rate,
            'expected_cars': best_results['avg_cars_won'],
            'expected_spending': best_results['avg_spent'],
            'budget_utilization': best_results['budget_utilization'],
            'total_combinations_evaluated': evaluated_combinations
        }
        
        for car_idx, bid_price in best_portfolio:
            car_info = candidates.iloc[car_idx]
            win_prob = self.calculate_win_probability(candidates.index[car_idx], bid_price)
            
            result['portfolio'].append({
                'car_index': candidates.index[car_idx],
                'brand': car_info['brand'],
                'model': car_info['model'],
                'year': int(car_info['year']),
                'mileage_km': int(car_info['mileage_km']),
                'avg_market_price': int(car_info['mean']),
                'bid_price': int(bid_price),
                'win_probability': round(win_prob, 3),
                'price_range': f"{int(car_info['min']):,} - {int(car_info['max']):,}원"
            })
        
        return result
    
    def analyze_portfolio(self, portfolio_result: Dict) -> None:
        """포트폴리오 결과 분석 및 출력"""
        print("\n" + "="*80)
        print("🚗 최적 포트폴리오 분석 결과")
        print("="*80)
        
        print(f"\n📊 전체 성과:")
        print(f"  • 목표 달성 확률: {portfolio_result['success_rate']:.1%}")
        print(f"  • 예상 확보 차량: {portfolio_result['expected_cars']:.1f}대")
        print(f"  • 예상 지출: {portfolio_result['expected_spending']:,.0f}원")
        print(f"  • 예산 활용률: {portfolio_result['budget_utilization']:.1%}")
        
        print(f"\n🎯 추천 포트폴리오:")
        total_bid = 0
        for i, car in enumerate(portfolio_result['portfolio'], 1):
            print(f"\n  [{i}] {car['brand']} {car['model']} ({car['year']})")
            print(f"      주행거리: {car['mileage_km']:,}km")
            print(f"      시장 평균가: {car['avg_market_price']:,}원")
            print(f"      추천 입찰가: {car['bid_price']:,}원")
            print(f"      낙찰 확률: {car['win_probability']:.1%}")
            print(f"      가격 범위: {car['price_range']}")
            total_bid += car['bid_price']
        
        print(f"\n💰 총 입찰 금액: {total_bid:,}원")
        print(f"📈 평가된 조합 수: {portfolio_result['total_combinations_evaluated']:,}개")

# 사용 예시
def example_usage():
    """사용 예시"""
    
    # 샘플 데이터 생성 (실제 데이터로 교체 필요)
    sample_data = {
        'brand': ['현대', '기아', '삼성', '현대', '기아'] * 10,
        'model': ['소나타', 'K5', 'SM6', '아반떼', 'K3'] * 10,
        'year': [2019, 2020, 2018, 2021, 2019] * 10,
        'transmission': ['오토'] * 50,
        'fuel': ['휘발유', '경유', '휘발유', '휘발유', '경유'] * 10,
        'displacement_cc': [2000, 2000, 2000, 1600, 1600] * 10,
        'mileage_km': np.random.randint(30000, 150000, 50),
        'color': ['흰색', '검정', '회색', '흰색', '파랑'] * 10,
        'auction_date': ['2025-01-01'] * 50,
        'auction_house': ['오토허브'] * 50,
        'min': np.random.randint(2000, 3000, 50) * 1000,
        'max': np.random.randint(3500, 4500, 50) * 1000,
        'mean': np.random.randint(2500, 4000, 50) * 1000,
        'std': np.random.randint(200, 800, 50) * 1000,
        'price_range': np.random.randint(500, 1500, 50) * 1000
    }
    
    # price_list 생성 (실제 낙찰가 리스트)
    price_lists = []
    for i in range(50):
        base_price = sample_data['mean'][i]
        n_prices = np.random.randint(3, 8)  # 3~7개의 가격 데이터
        prices = np.random.normal(base_price, sample_data['std'][i] * 0.3, n_prices)
        prices = [max(price, base_price * 0.7) for price in prices]  # 최소값 제한
        price_lists.append(prices)
    
    sample_data['price_list'] = price_lists
    
    df = pd.DataFrame(sample_data)
    
    # 옵티마이저 초기화
    optimizer = CarAuctionOptimizer(df)
    
    # 최적 포트폴리오 찾기
    print("포트폴리오 최적화 시작...")
    result = optimizer.find_optimal_portfolio(
        budget=10000000,  # 1000만원
        target_cars=2,    # 2대 목표
        max_candidates=30,
        min_reliability=3
    )
    
    # 결과 분석
    optimizer.analyze_portfolio(result)
    
    return optimizer, result

# 실제 사용법
if __name__ == "__main__":
    # 실제 데이터를 로드하여 사용
    # df = pd.read_csv('your_auction_data.csv')
    # optimizer = CarAuctionOptimizer(df)
    # 
    # result = optimizer.find_optimal_portfolio(
    #     budget=10000000,  # 1000만원
    #     target_cars=2     # 2대
    # )
    # 
    # optimizer.analyze_portfolio(result)
    
    # 예시 실행
    optimizer, result = example_usage()