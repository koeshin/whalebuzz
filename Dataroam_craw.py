import pandas as pd
from playwright.sync_api import sync_playwright
from io import StringIO
import time
import random
import re

# 1. 대상 리스트 (검증된 코드 사용)
TARGET_GURUS = [
    {"code": "SAM",     "name": "Scion Asset Mgmt (Michael Burry)"},
    {"code": "BAUPOST", "name": "Baupost Group (Seth Klarman)"}, 
    {"code": "BRK",     "name": "Berkshire Hathaway (Warren Buffett)"},
    # 필요한 만큼 추가
]

def scrape_ticker_history():
    all_history_data = []

    with sync_playwright() as p:
        # 브라우저 실행
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = context.new_page()

        print(f"🔥 [종목 중심] 히스토리 수집을 시작합니다.\n")

        for guru in TARGET_GURUS:
            guru_code = guru["code"]
            guru_name = guru["name"]
            
            print(f"--- [{guru_name}] 종목 발굴 시작 ---")

            # Step 1: Activity 페이지에서 '건드린 종목' 리스트 확보
            # typ=a (All)로 해야 매수/매도한 모든 종목을 찾을 수 있습니다.
            url_activity = f"https://www.dataroma.com/m/m_activity.php?m={guru_code}&typ=a"
            
            unique_tickers = set() # 중복 제거를 위한 Set
            
            try:
                page.goto(url_activity, timeout=30000)
                page.wait_for_selector("#grid", timeout=10000)
                
                # 테이블 내의 모든 링크(a 태그) 중에서 'hist.php'로 가는 것만 찾음
                # href 예시: hist.php?f=SAM&s=LULU
                links = page.locator("#grid td.stock a").all()
                
                for link in links:
                    href = link.get_attribute("href")
                    # 정규식으로 티커(s=???) 추출
                    match = re.search(r's=([^&]+)', href)
                    if match:
                        ticker = match.group(1)
                        unique_tickers.add(ticker)
                
                print(f"   👉 총 {len(unique_tickers)}개의 고유 종목 발견: {list(unique_tickers)[:5]} ...")

            except Exception as e:
                print(f"   ❌ Activity 페이지 접속 실패: {e}")
                continue

            # Step 2: 각 티커별 상세 히스토리 페이지 순회
            # (티커가 많으면 시간이 오래 걸리므로, 진행 상황을 보여줌)
            count = 0
            for ticker in unique_tickers:
                count += 1
                history_url = f"https://www.dataroma.com/m/hist/hist.php?f={guru_code}&s={ticker}"
                print(f"   [{count}/{len(unique_tickers)}] {ticker} 분석 중...", end="\r") # 한 줄로 출력

                try:
                    page.goto(history_url, timeout=20000)
                    # 히스토리 테이블 대기
                    try:
                        page.wait_for_selector("#grid", timeout=5000)
                    except:
                        # 데이터가 없는 경우도 있음
                        continue

                    html = page.content()
                    dfs = pd.read_html(StringIO(html))
                    
                    if dfs:
                        hist_df = dfs[0]
                        
                        # 컬럼 정리 (Period, Shares, % of Portfolio, Activity, % Change, Price, Value 등)
                        # 사이트 구조상 컬럼명이 조금씩 다를 수 있어 핵심만 남김
                        
                        # 메타데이터 추가
                        hist_df.insert(0, "Manager", guru_name)
                        hist_df.insert(1, "Ticker", ticker)
                        
                        all_history_data.append(hist_df)
                        
                except Exception as e:
                    # 특정 종목 실패해도 계속 진행
                    pass
                
                # 서버 부하 방지를 위한 딜레이 (필수)
                time.sleep(random.uniform(1.0, 2.0))
            
            print(f"\n   ✅ {guru_name} 완료.\n")

        browser.close()

    # 결과 저장
    if all_history_data:
        print("\n📊 데이터 병합 중...")
        master_df = pd.concat(all_history_data, ignore_index=True)
        
        # 보기 좋게 컬럼 정리 (옵션)
        # 보통 컬럼: Period, Shares, % of Portfolio, Activity, % Change to Portfolio, Reported Price
        
        filename = "Guru_Ticker_Full_History.csv"
        master_df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"🎉 수집 완료! '{filename}' 저장됨. (총 {len(master_df)}건의 거래 기록)")
        
        # 샘플 출력
        print(master_df.head())
        return master_df
    else:
        print("수집된 데이터가 없습니다.")
        return None

if __name__ == "__main__":
    scrape_ticker_history()