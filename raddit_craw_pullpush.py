"""
Reddit 주식 티커 크롤러 - PullPush.io API 사용

PullPush.io는 Pushshift의 후속 서비스로 2023년 데이터 크롤링 가능
API 키 불필요, 무료 사용 가능
"""

import requests
import pandas as pd
import re
from datetime import datetime, timedelta
import time
import json
from typing import List, Dict, Set
from collections import defaultdict

class RedditTickerCrawler:
    """
    PullPush.io API를 사용한 Reddit 크롤러
    - 2023년 분기별 데이터 크롤링 가능
    - API 키 불필요
    - Rate Limit: 15 req/min (soft), 30 req/min (hard), 1000 req/hr (장기)
    """
    
    def __init__(self):
        self.base_url = "https://api.pullpush.io/reddit/search/submission"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'RedditTickerCrawler/1.0'
        })
        
        # Rate limiting 관리
        self.request_count = 0
        self.request_times = []
        
        self.subreddits_config = {
            'wallstreetbets': {
                'style': '광기 & 하이프',
                'strategy': 'Activist / Growth',
                'characteristics': '단기 급등(Pump), 숏 스퀴즈, 밈(Meme) 화력 측정'
            },
            'stocks': {
                'style': '일반 투자 토론',
                'strategy': 'All Round',
                'characteristics': '균형 잡힌 시각, 진지한 뉴스 공유'
            },
            'investing': {
                'style': '장기/펀더멘털',
                'strategy': 'Value',
                'characteristics': '가치주 심층 분석'
            }
        }
    
    def rate_limit_wait(self):
        """Rate limit 관리 (15 req/min soft limit)"""
        current_time = time.time()
        
        # 1분 이내의 요청만 유지
        self.request_times = [t for t in self.request_times if current_time - t < 60]
        
        # 15개 이상이면 대기
        if len(self.request_times) >= 14:
            wait_time = 60 - (current_time - self.request_times[0]) + 1
            if wait_time > 0:
                print(f"  Rate limit 대기: {wait_time:.1f}초")
                time.sleep(wait_time)
                self.request_times = []
        
        # 요청 간 최소 간격 (안전하게 4초)
        time.sleep(4)
        self.request_times.append(time.time())
    
    def extract_tickers(self, text: str, target_tickers: Set[str]) -> List[str]:
        """텍스트에서 타겟 티커 추출"""
        if not text:
            return []
        text_upper = text.upper()
        found = []
        for ticker in target_tickers:
            # 단어 경계를 고려한 매칭
            if re.search(r'\b' + re.escape(ticker) + r'\b', text_upper):
                found.append(ticker)
        return found
    
    def get_quarter_timestamps(self, year: int, quarter: int):
        """분기의 시작/종료 Unix timestamp 반환"""
        quarter_starts = {
            1: (1, 1), 2: (4, 1), 3: (7, 1), 4: (10, 1)
        }
        
        start_month, start_day = quarter_starts[quarter]
        start_date = datetime(year, start_month, start_day)
        
        if quarter == 4:
            end_date = datetime(year + 1, 1, 1) - timedelta(seconds=1)
        else:
            next_month, _ = quarter_starts[quarter + 1]
            end_date = datetime(year, next_month, 1) - timedelta(seconds=1)
        
        return int(start_date.timestamp()), int(end_date.timestamp())
    
    def crawl_quarter(self, subreddit_name: str, year: int, quarter: int,
                      target_tickers: Set[str], target_count: int = 1000) -> List[Dict]:
        """
        특정 서브레딧의 분기별 데이터 크롤링
        
        Args:
            subreddit_name: 서브레딧 이름
            year: 연도
            quarter: 분기 (1-4)
            target_tickers: 찾을 티커 세트
            target_count: 목표 데이터 수 (티커 매칭된 것)
            
        Returns:
            크롤링된 데이터 리스트
        """
        print(f"\n{'='*60}")
        print(f"크롤링: r/{subreddit_name} - {year}년 Q{quarter}")
        print(f"{'='*60}")
        
        start_ts, end_ts = self.get_quarter_timestamps(year, quarter)
        results = []
        
        params = {
            'subreddit': subreddit_name,
            'after': start_ts,
            'before': end_ts,
            'sort': 'desc',
            'sort_type': 'score',  # 인기순 (score 기준)
            'size': 100  # 한 번에 100개씩 가져오기
        }
        
        matched_count = 0
        total_processed = 0
        before_timestamp = end_ts
        
        while matched_count < target_count:
            params['before'] = before_timestamp
            
            try:
                self.rate_limit_wait()
                
                response = self.session.get(self.base_url, params=params, timeout=30)
                
                if response.status_code != 200:
                    print(f"  ⚠️  HTTP {response.status_code} 에러")
                    if response.status_code == 429:
                        print("  Rate limit 초과, 60초 대기...")
                        time.sleep(60)
                        continue
                    break
                
                data = response.json()
                posts = data.get('data', [])
                
                if not posts:
                    print(f"  더 이상 데이터 없음")
                    break
                
                for post in posts:
                    total_processed += 1
                    
                    title = post.get('title', '')
                    selftext = post.get('selftext', '')
                    combined_text = f"{title} {selftext}"
                    
                    found_tickers = self.extract_tickers(combined_text, target_tickers)
                    
                    if found_tickers:
                        for ticker in found_tickers:
                            created_utc = post.get('created_utc', 0)
                            
                            post_data = {
                                'source': 'reddit',
                                'subreddit': subreddit_name,
                                'subreddit_style': self.subreddits_config[subreddit_name]['style'],
                                'subreddit_strategy': self.subreddits_config[subreddit_name]['strategy'],
                                'ticker': ticker,
                                'title': title,
                                'selftext': selftext[:1000] if selftext else '',
                                'upvote_ratio': post.get('upvote_ratio', 0),
                                'score': post.get('score', 0),
                                'num_comments': post.get('num_comments', 0),
                                'created_utc': created_utc,
                                'created_date': datetime.fromtimestamp(created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                                'year': year,
                                'quarter': quarter,
                                'author': post.get('author', '[deleted]'),
                                'author_flair_text': post.get('author_flair_text', None),
                                'url': post.get('url', ''),
                                'permalink': f"https://reddit.com{post.get('permalink', '')}"
                            }
                            results.append(post_data)
                            matched_count += 1
                    
                    # 다음 페이지를 위한 timestamp 업데이트
                    before_timestamp = post.get('created_utc', before_timestamp)
                
                print(f"  📊 처리: {total_processed}개 | 매칭: {matched_count}/{target_count}개")
                
                # 목표 달성 시 종료
                if matched_count >= target_count:
                    break
                
            except requests.exceptions.Timeout:
                print(f"  ⏱️  타임아웃, 재시도...")
                time.sleep(5)
                continue
                
            except Exception as e:
                print(f"  ❌ 에러: {str(e)}")
                break
        
        print(f"✅ 완료: {len(results)}개 데이터 수집")
        return results
    
    def crawl_all_quarters(self, start_year: int, end_year: int,
                          target_tickers: Set[str], 
                          posts_per_quarter: int = 1000) -> pd.DataFrame:
        """
        모든 서브레딧의 분기별 데이터 크롤링
        
        Args:
            start_year: 시작 연도
            end_year: 종료 연도
            target_tickers: 찾을 티커 세트
            posts_per_quarter: 분기당 목표 게시물 수
            
        Returns:
            전체 데이터 DataFrame
        """
        all_data = []
        current_year = datetime.now().year
        current_quarter = (datetime.now().month - 1) // 3 + 1
        
        total_tasks = 0
        for year in range(start_year, end_year + 1):
            for quarter in range(1, 5):
                if year == current_year and quarter > current_quarter:
                    continue
                total_tasks += len(self.subreddits_config)
        
        completed = 0
        
        for year in range(start_year, end_year + 1):
            for quarter in range(1, 5):
                # 미래 분기는 스킵
                if year == current_year and quarter > current_quarter:
                    continue
                
                for subreddit_name in self.subreddits_config.keys():
                    completed += 1
                    print(f"\n\n📍 진행률: {completed}/{total_tasks}")
                    
                    try:
                        quarter_data = self.crawl_quarter(
                            subreddit_name, year, quarter,
                            target_tickers, posts_per_quarter
                        )
                        all_data.extend(quarter_data)
                        
                    except KeyboardInterrupt:
                        print("\n\n⚠️  사용자에 의해 중단됨")
                        print(f"현재까지 수집된 데이터: {len(all_data)}개")
                        if all_data:
                            return pd.DataFrame(all_data)
                        raise
                        
                    except Exception as e:
                        print(f"❌ 에러: r/{subreddit_name} {year}Q{quarter} - {str(e)}")
                        continue
        
        return pd.DataFrame(all_data)
    
    def save_data(self, df: pd.DataFrame, base_filename: str = 'reddit_ticker_data'):
        """
        데이터를 CSV와 JSON으로 저장
        
        Args:
            df: 저장할 DataFrame
            base_filename: 기본 파일명
        """
        if df.empty:
            print("⚠️  저장할 데이터가 없습니다.")
            return
        
        # CSV 저장
        csv_file = f"{base_filename}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"\n💾 CSV 저장: {csv_file}")
        
        # JSON 저장
        json_file = f"{base_filename}.json"
        df.to_json(json_file, orient='records', force_ascii=False, indent=2)
        print(f"💾 JSON 저장: {json_file}")
        
        # 통계 출력
        print(f"\n{'='*60}")
        print(f"📊 크롤링 통계")
        print(f"{'='*60}")
        print(f"총 데이터 수: {len(df):,}개\n")
        
        print("📌 서브레딧별 분포:")
        print(df['subreddit'].value_counts().to_string())
        
        print(f"\n📈 티커별 분포:")
        print(df['ticker'].value_counts().head(20).to_string())
        
        print(f"\n📅 연도/분기별 분포:")
        quarter_dist = df.groupby(['year', 'quarter']).size().sort_index()
        print(quarter_dist.to_string())
        
        print(f"\n💪 평균 Score (화력):")
        avg_score = df.groupby('subreddit')['score'].mean().round(1)
        print(avg_score.to_string())
        
        print(f"\n💬 평균 댓글 수:")
        avg_comments = df.groupby('subreddit')['num_comments'].mean().round(1)
        print(avg_comments.to_string())


# ============================================================================
# 사용 예시
# ============================================================================
if __name__ == "__main__":
    print("="*60)
    print("Reddit 주식 티커 크롤러 v2.0 (PullPush.io)")
    print("="*60)
    
    # 크롤러 초기화
    crawler = RedditTickerCrawler()
    
    # 타겟 티커 설정
    target_tickers = {
        # 주요 테크 주식
        'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA', 'NVDA', 'AMD',
        
        # 유명 밈주
        'GME', 'AMC', 'BB', 'BBBY', 'NOK',
        
        # 에너지/석유
        'OXY', 'XOM', 'CVX', 'COP',
        
        # 기타 인기 종목
        'PLTR', 'BABA', 'NIO', 'SOFI', 'COIN', 'HOOD',
        
        # ETF
        'SPY', 'QQQ', 'IWM', 'DIA', 'VOO'
    }
    
    print(f"\n🎯 타겟 티커 ({len(target_tickers)}개):")
    print(f"{', '.join(sorted(target_tickers))}\n")
    
    # 크롤링 파라미터
    START_YEAR = 2023
    END_YEAR = 2024
    POSTS_PER_QUARTER = 1000  # 분기당 목표 개수 (티커 매칭된 것)
    
    print(f"📅 기간: {START_YEAR}년 ~ {END_YEAR}년 (분기별)")
    print(f"📊 목표: 분기당 {POSTS_PER_QUARTER}개 (서브레딧당)")
    print(f"⏱️  예상 소요시간: {(END_YEAR-START_YEAR+1)*4*3*5} ~ 10분")
    print(f"\n⚠️  Rate Limit: 시간당 1000 요청, 분당 15 요청")
    print(f"💡 중단하려면 Ctrl+C를 누르세요 (현재까지 데이터는 저장됨)\n")
    
    input("🚀 Enter를 눌러 크롤링 시작...")
    
    try:
        # 크롤링 시작
        start_time = time.time()
        
        df = crawler.crawl_all_quarters(
            start_year=START_YEAR,
            end_year=END_YEAR,
            target_tickers=target_tickers,
            posts_per_quarter=POSTS_PER_QUARTER
        )
        
        elapsed = time.time() - start_time
        print(f"\n\n⏱️  총 소요시간: {elapsed/60:.1f}분")
        
        # 데이터 저장
        if not df.empty:
            crawler.save_data(df, f'reddit_ticker_data_{START_YEAR}_{END_YEAR}')
            print(f"\n✅ 모든 작업 완료!")
        else:
            print(f"\n⚠️  수집된 데이터가 없습니다.")
            
    except KeyboardInterrupt:
        print("\n\n⚠️  사용자에 의해 중단됨")
        if not df.empty:
            crawler.save_data(df, f'reddit_ticker_data_partial_{START_YEAR}_{END_YEAR}')
            print(f"✅ 부분 데이터 저장 완료")
    
    except Exception as e:
        print(f"\n❌ 치명적 에러: {str(e)}")
        import traceback
        traceback.print_exc()