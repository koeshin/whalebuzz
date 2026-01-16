import pandas as pd
from playwright.sync_api import sync_playwright
from io import StringIO
import time
import random

# 1. 9대 거인 리스트
TARGET_GURUS = [
    {"code": "BRK", "name": "Berkshire Hathaway", "style": "Value"},
    {"code": "BAUPOST", "name": "Baupost Group",       "style": "Value"},
    {"code": "SAM", "name": "Scion Asset Mgmt",    "style": "Value"},
    {"code": "TGM", "name": "Tiger Global",        "style": "Growth"},
    {"code": "COAT", "name": "Coatue Management",  "style": "Growth"}, 
    {"code": "DA",  "name": "Duquesne Family",     "style": "Growth"},
    {"code": "PSC", "name": "Pershing Square",     "style": "Activist"},
    {"code": "IC",  "name": "Icahn Enterprises",   "style": "Activist"},
    {"code": "TP",  "name": "Third Point",         "style": "Activist"},
]

# 2. 수집할 기간 (2024년 1분기 ~ 2025년 4분기)
# 현재 시점(2026년 1월) 기준, 과거 데이터를 모두 봅니다.
QUARTERS = [
    "2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31",
    "2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"
]

def clean_number(value):
    """문자열($, %, ,)을 숫자(float)로 변환하는 헬퍼 함수"""
    if isinstance(value, str):
        value = value.replace('$', '').replace('%', '').replace(',', '').strip()
        try:
            return float(value)
        except:
            return 0.0
    return value

def scrape_history_portfolios():
    all_dfs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = context.new_page()

        print(f"⏳ Time Machine 가동: 총 {len(TARGET_GURUS)}명 * {len(QUARTERS)}분기 데이터 수집 시작...\n")

        for guru in TARGET_GURUS:
            code = guru["code"]
            name = guru["name"]
            
            print(f"--- [{name}] History Scanning ---")

            for period in QUARTERS:
                # [핵심] 날짜 파라미터(p)를 URL에 추가하여 과거 데이터 접근
                url = f"https://www.dataroma.com/m/holdings.php?m={code}&p={period}"
                
                try:
                    page.goto(url, timeout=20000)
                    
                    # 데이터가 없는 경우(설립 전이거나 보고 누락 등) 대비
                    try:
                        page.wait_for_selector("#grid", timeout=3000)
                    except:
                        print(f"   [Skip] {period}: 데이터 없음 (or 로딩 실패)")
                        continue

                    html = page.content()
                    dfs = pd.read_html(StringIO(html))
                    raw_df = dfs[0]

                    # 컬럼 인덱스로 데이터 추출 (안전장치)
                    if len(raw_df.columns) >= 6:
                        df_subset = raw_df.iloc[:, :6].copy()
                        df_subset.columns = ['Stock_Name', 'Ticker', 'Weight_Pct', 'Shares', 'Price', 'Value']
                        
                        # 메타데이터 추가
                        df_subset.insert(0, "Manager", name)
                        df_subset.insert(1, "Style", guru["style"])
                        df_subset.insert(2, "Report_Date", period) # 기준일자 중요!
                        
                        # 데이터 정제 (숫자 변환)
                        df_subset['Weight_Pct'] = df_subset['Weight_Pct'].apply(clean_number)
                        df_subset['Value'] = df_subset['Value'].apply(clean_number)
                        # Shares는 가끔 문자가 섞일 수 있어 처리
                        df_subset['Shares'] = df_subset['Shares'].apply(clean_number)

                        all_dfs.append(df_subset)
                        print(f"   ✅ {period}: {len(df_subset)}개 종목 수집")
                    
                    else:
                        print(f"   ⚠️ {period}: 테이블 구조 이상")

                except Exception as e:
                    print(f"   ❌ {period}: 에러 ({e})")
                
                # 서버 부하 방지를 위한 랜덤 딜레이 (필수!)
                time.sleep(random.uniform(1.5, 3.0))

        browser.close()

    # 결과 저장
    if all_dfs:
        print("\n📊 데이터 병합 및 CSV 저장 중...")
        master_df = pd.concat(all_dfs, ignore_index=True)
        
        # 날짜순, 매니저순 정렬
        master_df = master_df.sort_values(by=['Manager', 'Report_Date'])
        
        filename = "Guru_Portfolios_TimeSeries_2024-2025.csv"
        master_df.to_csv(filename, index=False, encoding="utf-8-sig")
        
        print(f"🎉 미션 성공! 총 {len(master_df)}행의 시계열 데이터가 '{filename}'에 저장되었습니다.")
        
        # 미리보기 (상위 5개)
        print(master_df.head())
        return master_df
    else:
        print("\n수집된 데이터가 없습니다.")
        return None

if __name__ == "__main__":
    scrape_history_portfolios()