import pandas as pd
from playwright.sync_api import sync_playwright
import time
import random

# 1. 수집 대상 리스트 (코드, 운용사명)
# Dataroma URL의 'm=' 뒤에 오는 코드를 입력합니다.
TARGET_GURUS = [
    {"code": "BRK", "name": "Warren Buffett (Berkshire)"},
    {"code": "PSC", "name": "Bill Ackman (Pershing Square)"},
    {"code": "AM",  "name": "David Tepper (Appaloosa)"},
    {"code": "IC",  "name": "Carl Icahn (Icahn Capital)"},
    {"code": "TP",  "name": "Daniel Loeb (Third Point)"},
    # 필요한 만큼 계속 추가 가능
]

def scrape_all_gurus():
    all_data_frames = [] # 수집된 데이터를 모을 리스트

    with sync_playwright() as p:
        # 브라우저 띄우기 (headless=True 권장)
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = context.new_page()

        print(f"🚀 총 {len(TARGET_GURUS)}명의 포트폴리오 수집을 시작합니다.\n")

        for guru in TARGET_GURUS:
            code = guru["code"]
            name = guru["name"]
            url = f"https://www.dataroma.com/m/holdings.php?m={code}"
            
            print(f"[{name}] 데이터 수집 중... ({url})")

            try:
                page.goto(url, timeout=30000)
                
                # 테이블 로딩 대기
                page.wait_for_selector("#grid", timeout=10000)
                
                # HTML 파싱
                html = page.content()
                dfs = pd.read_html(html)
                
                # 포트폴리오 테이블 가져오기 (보통 첫 번째 혹은 내용이 가장 많은 테이블)
                portfolio_df = dfs[0]

                # **핵심: 누구의 데이터인지 식별자 컬럼 추가**
                portfolio_df.insert(0, "Manager_Code", code)
                portfolio_df.insert(1, "Manager_Name", name)
                
                # 수집 날짜(Reference) 추가 (실제로는 13F 보고 기준일을 파싱해야 하지만, 지금은 수집일로 대체)
                portfolio_df["Scraped_Date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

                # 리스트에 추가
                all_data_frames.append(portfolio_df)
                print(f"   ✅ 성공! ({len(portfolio_df)}개 종목)")

            except Exception as e:
                print(f"   ❌ 실패: {e}")
            
            # 차단 방지를 위한 랜덤 딜레이 (2~5초)
            time.sleep(random.uniform(2, 5))

        browser.close()

    # 2. 데이터 병합 및 저장
    if all_data_frames:
        print("\n📊 데이터 병합 중...")
        # 모든 DataFrame을 위아래로 합치기 (Concat)
        master_df = pd.concat(all_data_frames, ignore_index=True)
        
        # 간단한 전처리: 컬럼명 공백 제거
        master_df.columns = [c.strip() for c in master_df.columns]

        # CSV 저장
        filename = "all_gurus_portfolio.csv"
        master_df.to_csv(filename, index=False, encoding="utf-8-sig") # 엑셀 깨짐 방지(utf-8-sig)
        print(f"🎉 완료! '{filename}' 파일에 총 {len(master_df)}개의 행이 저장되었습니다.")
        
        return master_df
    else:
        print("수집된 데이터가 없습니다.")
        return None

if __name__ == "__main__":
    scrape_all_gurus()