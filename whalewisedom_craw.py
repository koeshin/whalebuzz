import pandas as pd
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
import time

TARGETS = [
    {"name": "Berkshire Hathaway", "slug": "berkshire-hathaway-inc"},
    {"name": "Bridgewater", "slug": "bridgewater-associates-lp"},
    {"name": "Scion Asset", "slug": "scion-asset-management-llc"},
]

def scrape_single_filer(target, headless=True):
    """단일 운용사 크롤링 (병렬 처리용)"""
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=['--disable-blink-features=AutomationControlled']
        )
        
        # 리소스 차단으로 속도 향상
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        
        page = context.new_page()
        
        # 이미지, 폰트, CSS 차단 (선택적)
        page.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2}", lambda route: route.abort())
        
        url = f"https://whalewisdom.com/filer/{target['slug']}"
        print(f"[{target['name']}] 크롤링 시작...")
        
        try:
            # 페이지 로딩 (타임아웃 단축)
            page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            # 테이블 대기 (동적)
            page.wait_for_selector("#holdings_table", timeout=10000)
            
            # 필요시에만 스크롤
            if page.locator(".lazy-load").count() > 0:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1000)
            
            # HTML 파싱
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            # 테이블 추출
            table = soup.select_one("#holdings_table")
            if not table:
                print(f"[{target['name']}] 테이블 없음")
                return None
            
            dfs = pd.read_html(StringIO(str(table)))
            
            if dfs:
                df = dfs[0].dropna(axis=1, how='all')
                df.insert(0, "Manager", target['name'])
                top20 = df.head(20)
                
                filename = f"Whale_{target['slug']}.csv"
                top20.to_csv(filename, index=False, encoding='utf-8-sig')
                print(f"✅ [{target['name']}] 완료 ({len(df)}개 중 20개 저장)")
                return top20
            
        except Exception as e:
            print(f"❌ [{target['name']}] 에러: {e}")
            return None
        finally:
            browser.close()

def scrape_whalewisdom_fast(parallel=False, headless=True):
    """
    parallel=True: 병렬 처리 (빠름, but 서버 부하 주의)
    headless=True: GUI 없이 실행 (Cloudflare 없을 때만)
    """
    if parallel:
        print("⚡ 병렬 모드 (최대 3개 동시 실행)")
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(
                lambda t: scrape_single_filer(t, headless), 
                TARGETS
            ))
        return [r for r in results if r is not None]
    else:
        print("🐌 순차 모드")
        results = []
        for target in TARGETS:
            result = scrape_single_filer(target, headless)
            if result is not None:
                results.append(result)
            time.sleep(1)  # 서버 부담 완화
        return results

if __name__ == "__main__":
    # 옵션 1: 병렬 + headless (가장 빠름, Cloudflare 없을 때)
    # results = scrape_whalewisdom_fast(parallel=True, headless=True)
    
    # 옵션 2: 순차 + headless (안정적)
    # results = scrape_whalewisdom_fast(parallel=False, headless=True)
    
    # 옵션 3: 순차 + GUI (Cloudflare 있을 때)
    results = scrape_whalewisdom_fast(parallel=False, headless=False)
    
    print(f"\n📊 총 {len(results)}개 운용사 데이터 수집 완료")