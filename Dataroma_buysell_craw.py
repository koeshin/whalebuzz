import pandas as pd
from playwright.sync_api import sync_playwright
from io import StringIO
import time
import random
import re
import os  # 파일 존재 여부 확인용

# 1. 대상 리스트
# 1. 대상 리스트 (총 21개, Dataroma 검증 완료)
TARGET_GURUS = [
    # --- Value (가치투자) ---
    {"code": "BRK",     "name": "Berkshire Hathaway (Buffett)", "style": "Value"},
    {"code": "BAUPOST", "name": "Baupost Group (Klarman)",     "style": "Value"},
    {"code": "SAM",     "name": "Scion Asset Mgmt (Burry)",    "style": "Value"},
    {"code": "HC",      "name": "Himalaya Capital (Li Lu)",    "style": "Value"},
    {"code": "PI",     "name": "Pabrai Investments(Pabrai)",       "style": "Value"}, ## 다시 돌리기
    {"code": "FS",      "name": "Fundsmith (Terry Smith)",     "style": "Value"},
    {"code": "oaklx",     "name": "Oakmark (Bill Nygren)",       "style": "Value"}, ## 다시 돌리기

    # --- Growth (성장주/Tiger Cubs) ---
    {"code": "TGM",     "name": "Tiger Global (Chase Coleman)","style": "Growth"},
    {"code": "AM",      "name": "Appaloosa (David Tepper)",    "style": "Growth"},
    {"code": "vg",     "name": "Viking Global (Halvorsen)",   "style": "Growth"}, ## 다시 돌리기
    {"code": "LPC",     "name": "Lone Pine (Stephen Mandel)",  "style": "Growth"},
    {"code": "MC",      "name": "Maverick Capital (Lee Ainslie)","style": "Growth"},
    {"code": "AC",    "name": "Akre Capital (Chuck Akre)",   "style": "Growth"}, # 다시 돌리기
    {"code": "tci",     "name": "TCI Fund (Chris Hohn)",       "style": "Growth"},

    # --- Activist / Deep Value (행동주의) ---
    {"code": "PSC",     "name": "Pershing Square (Ackman)",    "style": "Activist"},
    {"code": "IC",      "name": "Icahn Capital (Carl Icahn)",  "style": "Activist"},
    {"code": "TP",      "name": "Third Point (Dan Loeb)",      "style": "Activist"},
    {"code": "GL",      "name": "Greenlight (David Einhorn)",  "style": "Activist"},
    {"code": "TRI",     "name": "Trian Partners (Nelson Peltz)","style": "Activist"},
    {"code": "STAR",    "name": "Starboard Value (Jeff Smith)","style": "Activist"},
    {"code": "FAIRX",   "name": "Fairholme (Bruce Berkowitz)", "style": "Activist"},
]
FILENAME = "Guru_History_21_Legends.csv"

def scrape_and_save_incremental():
    # 시작 전에 기존 파일이 있다면 안내 메시지 (혹은 삭제)
    if os.path.exists(FILENAME):
        print(f"ℹ️ 알림: '{FILENAME}' 파일이 이미 존재합니다. 뒤에 이어서 저장합니다.")
    else:
        print(f"ℹ️ 알림: 새로운 파일 '{FILENAME}'을 생성합니다.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # Context를 한 번 만들고 계속 재사용하되, 페이지는 닫아줍니다.
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )

        print(f"\n🔥 [안전 모드] 한 명씩 수집하고 즉시 저장합니다.\n")

        for i, guru in enumerate(TARGET_GURUS):
            guru_code = guru["code"]
            guru_name = guru["name"]
            guru_style = guru["style"]
            
            # [중요] 한 명분 데이터를 담을 임시 리스트 (매번 초기화됨)
            current_guru_data = []
            
            print(f"--- [{i+1}/{len(TARGET_GURUS)}] {guru_name} ({guru_style}) 시작 ---")

            page = context.new_page()
            
            try:
                # 1. Activity 페이지 접속 & 티커 수집
                url_activity = f"https://www.dataroma.com/m/m_activity.php?m={guru_code}&typ=a"
                unique_tickers = set()
                
                try:
                    page.goto(url_activity, timeout=30000)
                    try:
                        page.wait_for_selector("#grid", timeout=5000)
                    except:
                        print("   ⚠️ 테이블 로딩 실패 (데이터 없음)")

                    # stock.php 링크 찾기
                    target_links = page.locator('a[href*="stock.php?sym="]').all()
                    for link in target_links:
                        href = link.get_attribute("href")
                        if href:
                            match = re.search(r'sym=([^&]+)', href)
                            if match:
                                unique_tickers.add(match.group(1))
                    
                    print(f"   👉 {len(unique_tickers)}개 종목 발견")

                except Exception as e:
                    print(f"   ❌ Activity 에러: {e}")

                # 2. 상세 히스토리 수집
                count = 0
                for ticker in unique_tickers:
                    count += 1
                    history_url = f"https://www.dataroma.com/m/hist/hist.php?f={guru_code}&s={ticker}"
                    
                    print(f"   [{count}/{len(unique_tickers)}] {ticker}...", end="\r")

                    try:
                        page.goto(history_url, timeout=20000)
                        try:
                            page.wait_for_selector("#grid", timeout=2000)
                        except:
                            continue 

                        html = page.content()
                        dfs = pd.read_html(StringIO(html))
                        
                        if dfs:
                            hist_df = max(dfs, key=len)
                            if len(hist_df) > 1:
                                # 메타데이터 삽입
                                hist_df.insert(0, "Manager", guru_name)
                                hist_df.insert(1, "Style", guru_style) 
                                hist_df.insert(2, "Ticker", ticker)
                                
                                # 임시 리스트에 추가
                                current_guru_data.append(hist_df)
                            
                    except Exception:
                        pass
                    
                    # 딜레이 (너무 빠르면 차단되므로 적절히 유지)
                    time.sleep(random.uniform(0.5, 0.8))

            except Exception as e:
                print(f"   ❌ 치명적 에러: {e}")
            
            finally:
                page.close() # 페이지 닫아서 메모리 확보

            # 3. [저장 단계] 한 명 끝날 때마다 파일에 쓰기
            if current_guru_data:
                print(f"\n   💾 {guru_name} 데이터 저장 중... ", end="")
                
                # DataFrame 변환
                df_to_save = pd.concat(current_guru_data, ignore_index=True)
                
                # 파일이 없으면 헤더 포함(True), 있으면 헤더 뺌(False)
                # mode='a'는 append(이어쓰기) 모드입니다.
                file_exists = os.path.exists(FILENAME)
                
                df_to_save.to_csv(
                    FILENAME, 
                    mode='a', 
                    header=not file_exists, # 파일이 없을 때만 헤더 작성
                    index=False, 
                    encoding="utf-8-sig"
                )
                
                print(f"완료! (+{len(df_to_save)}행)")
                
                # 메모리에서 삭제 (Explicit Garbage Collection)
                del df_to_save
                del current_guru_data
            else:
                print(f"\n   ⚠️ 저장할 데이터가 없습니다.")
            
            print("------------------------------------------------")

        browser.close()
        print(f"\n🎉 모든 작업 종료. 결과 파일: {FILENAME}")

if __name__ == "__main__":
    scrape_and_save_incremental()