import pandas as pd
from playwright.sync_api import sync_playwright
import yfinance as yf
from io import StringIO
import time

# 분석 대상: 워런 버핏 (Berkshire Hathaway)
GURU_CODE = "BRK"
GURU_NAME = "Warren Buffett"

def get_guru_data():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # --- 1. 포트폴리오 (Holdings) 가져오기 ---
        print(f"[{GURU_NAME}] 포트폴리오 수집 중...")
        url_holdings = f"https://www.dataroma.com/m/holdings.php?m={GURU_CODE}"
        page.goto(url_holdings)
        page.wait_for_selector("#grid", timeout=10000)
        
        html_holdings = page.content()
        df_holdings = pd.read_html(StringIO(html_holdings))[0]
        
        # [디버깅] 실제 컬럼명이 무엇인지 확인 (나중에 문제 생기면 이 로그를 보세요)
        print("   👉 수집된 컬럼 목록:", df_holdings.columns.tolist())

        # [수정] 컬럼 이름 대신 '순서(Index)'로 선택하여 오류 방지
        # 보통 Dataroma 순서: [0:Stock, 1:Symbol, 2:% Portfolio, 3:Shares, 4:Price, 5:Value, 6:% Change...]
        # 안전하게 컬럼 이름을 강제로 변경합니다.
        
        # 필요한 컬럼 개수만큼만 슬라이싱해서 이름을 덮어씌웁니다.
        # (테이블 구조가 조금 달라도 앞쪽 6개 데이터는 보통 고정입니다)
        target_cols = ['Name', 'Ticker', 'Weight(%)', 'Shares', 'Price', 'Value($)']
        
        # 데이터프레임의 앞쪽 컬럼들을 우리가 원하는 이름으로 매핑
        df_subset = df_holdings.iloc[:, :6].copy() 
        df_subset.columns = target_cols

        # 필요한 것만 남김
        portfolio = df_subset[['Ticker', 'Name', 'Weight(%)', 'Value($)']].copy()
        
        print(f"   ✅ 보유 종목 {len(portfolio)}개 확보")

        # --- 2. 성과 (Performance) 가져오기 ---
        print(f"[{GURU_NAME}] 연도별 수익률 수집 중...")
        url_perf = f"https://www.dataroma.com/m/perf.php?m={GURU_CODE}"
        page.goto(url_perf)
        
        try:
            html_perf = page.content()
            df_perf = pd.read_html(StringIO(html_perf))[0]
            print(f"   ✅ 성과 데이터 확보 ({len(df_perf)}년치)")
        except:
            print("   ⚠️ 성과 데이터를 찾지 못했습니다.")
            df_perf = pd.DataFrame()

        browser.close()
        return portfolio, df_perf

def enrich_with_yfinance(portfolio_df):
    print("\n[Yahoo Finance] 섹터 및 세부 정보 연동 중 (시간이 좀 걸립니다)...")
    
    sectors = []
    current_prices = []
    
    tickers = portfolio_df['Ticker'].tolist()
    
    # 팁: yfinance는 Tickers를 한 번에 요청하면 더 빠릅니다.
    # 하지만 종목이 너무 많으면 나눌 필요가 있습니다. 여기선 단순하게 loop 돕니다.
    for ticker in tickers[:10]: # 테스트를 위해 상위 10개만 먼저 해봅니다. (전체 하려면 [:10] 제거)
        try:
            # '.'이 들어간 티커 수정 (예: BRK.B -> BRK-B)
            safe_ticker = ticker.replace(".", "-")
            stock = yf.Ticker(safe_ticker)
            
            # 정보 가져오기 (fast_info가 더 빠름)
            info = stock.info 
            
            sec = info.get('sector', 'Unknown')
            price = info.get('currentPrice', 0)
            
            sectors.append(sec)
            current_prices.append(price)
            print(f"   Finished: {ticker} -> {sec}")
            
        except Exception as e:
            print(f"   Error: {ticker}")
            sectors.append("Error")
            current_prices.append(0)
    
    # 데이터프레임에 붙이기 (상위 10개만 했으므로 길이 맞춤 주의)
    # 실제 사용 시에는 전체 리스트를 돌리세요.
    portfolio_df = portfolio_df.iloc[:len(sectors)].copy()
    portfolio_df['Sector'] = sectors
    portfolio_df['Current_Price'] = current_prices
    
    return portfolio_df

# --- 실행 ---
if __name__ == "__main__":
    # 1. Dataroma 크롤링
    pf_df, perf_df = get_guru_data()
    
    # 2. Yahoo Finance 데이터 결합
    final_df = enrich_with_yfinance(pf_df)
    
    # 3. 결과 출력 및 저장
    print("\n--- [Final Result: Top 5 Holdings] ---")
    print(final_df.head())
    
    print("\n--- [Manager Performance] ---")
    print(perf_df.head())


    # CSV 저장
    final_df.to_csv("Buffett_Enriched_Portfolio.csv", index=False, encoding='utf-8-sig')
    perf_df.to_csv("Buffett_Performance_History.csv", index=False, encoding='utf-8-sig')
    print("\n🎉 모든 데이터 저장 완료!")