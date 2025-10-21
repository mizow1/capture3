#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Webサイトの階層構造を取得してCSV/HTML形式で出力するクローラー
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urldefrag
import re
import csv
from collections import defaultdict
from typing import Set, Dict, List, Tuple
import time


class SitemapCrawler:
    """サイトマップを作成するクローラー"""

    def __init__(self, base_url: str):
        """
        Args:
            base_url: クロール開始URL
        """
        # base_urlを正規化
        parsed = urlparse(base_url)
        path = parsed.path
        if not path.endswith('/') and '.' not in path.split('/')[-1]:
            path = path + '/' if path else '/'
        self.base_url = f"{parsed.scheme}://{parsed.netloc}{path}"

        self.base_domain = parsed.netloc
        self.visited_urls: Set[str] = set()
        self.url_tree: Dict[str, List[Dict]] = defaultdict(list)
        self.url_titles: Dict[str, str] = {}
        self.url_depths: Dict[str, int] = {}
        self.failed_urls: Dict[str, str] = {}  # URL: エラー理由

        # 除外する拡張子
        self.excluded_extensions = {
            # 画像
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp', '.ico',
            # ドキュメント
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.txt', '.csv', '.zip', '.rar', '.7z',
            # バイナリ
            '.exe', '.dmg', '.pkg', '.deb', '.rpm',
            # 動画・音声
            '.mp4', '.avi', '.mov', '.mp3', '.wav',
            # その他
            '.xml', '.json', '.css', '.js'
        }

        # 許可する末尾パターン
        self.allowed_endings = ['/', '.html', '.htm', '.php']

        # HTTPヘッダー
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        # base_urlのパスを正規化（末尾スラッシュを除去）
        self.base_path = urlparse(self.base_url).path.rstrip('/')

    def normalize_url(self, url: str) -> str:
        """
        URLを正規化（末尾スラッシュありに統一）

        Args:
            url: 正規化するURL

        Returns:
            正規化されたURL
        """
        parsed = urlparse(url)
        path = parsed.path

        # パスが空の場合は/を設定
        if not path:
            path = '/'
        # 拡張子がない、またはディレクトリの場合は末尾に/を追加
        elif not path.endswith('/'):
            # 最後のセグメントに拡張子があるかチェック
            last_segment = path.split('/')[-1]
            if '.' not in last_segment:
                # 拡張子がない場合は/を追加
                path = path + '/'

        # URLを再構築
        normalized = f"{parsed.scheme}://{parsed.netloc}{path}"
        if parsed.query:
            normalized += f"?{parsed.query}"

        return normalized

    def is_valid_url(self, url: str) -> bool:
        """
        URLが取得対象かどうかを判定

        Args:
            url: チェックするURL

        Returns:
            有効なURLの場合True
        """
        # フラグメント(#)を除去
        url, _ = urldefrag(url)

        # 空URLはスキップ
        if not url:
            return False

        # 同一ドメインのみ
        parsed = urlparse(url)
        if parsed.netloc != self.base_domain:
            return False

        # URL引数付きはスキップ
        if '?' in url:
            return False

        # 除外拡張子チェック
        path_lower = parsed.path.lower()
        for ext in self.excluded_extensions:
            if path_lower.endswith(ext):
                return False

        # 許可される末尾パターンをチェック
        has_allowed_ending = any(path_lower.endswith(ending) for ending in self.allowed_endings)

        # 拡張子なしの場合
        if '.' not in parsed.path.split('/')[-1]:
            # 末尾が/でない場合は後でContent-Typeチェックが必要
            return True

        # 拡張子ありの場合は許可パターンのみOK
        return has_allowed_ending

    def get_page_content(self, url: str) -> Tuple[str, str, str, str]:
        """
        ページのHTMLコンテンツとタイトルを取得

        Args:
            url: 取得するURL

        Returns:
            (HTML文字列, ページタイトル, 最終URL, エラーメッセージ)のタプル
            リダイレクトされた場合は最終URLを返す
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=10)

            # Content-Typeチェック
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' not in content_type:
                return None, None, None, f"Content-Type: {content_type}"

            # ステータスコードチェック
            if response.status_code != 200:
                return None, None, None, f"HTTP {response.status_code}"

            soup = BeautifulSoup(response.content, 'html.parser')

            # タイトル取得
            title = ''
            if soup.title:
                title = soup.title.string.strip() if soup.title.string else ''

            # 最終URLを取得（リダイレクト後）
            final_url = response.url

            return response.text, title, final_url, None

        except requests.exceptions.Timeout:
            error_msg = "タイムアウト"
            print(f"エラー: {url} - {error_msg}")
            return None, None, None, error_msg
        except requests.exceptions.ConnectionError:
            error_msg = "接続エラー"
            print(f"エラー: {url} - {error_msg}")
            return None, None, None, error_msg
        except Exception as e:
            error_msg = str(e)
            print(f"エラー: {url} - {error_msg}")
            return None, None, None, error_msg

    def extract_links(self, html: str, current_url: str) -> Set[str]:
        """
        HTML内のリンクを抽出

        Args:
            html: HTML文字列
            current_url: 現在のページURL

        Returns:
            抽出されたURLのセット
        """
        soup = BeautifulSoup(html, 'html.parser')
        links = set()

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            # 絶対URLに変換
            absolute_url = urljoin(current_url, href)

            # フラグメントを除去
            absolute_url, _ = urldefrag(absolute_url)

            if self.is_valid_url(absolute_url):
                # URLを正規化（末尾スラッシュ統一）
                normalized_url = self.normalize_url(absolute_url)
                links.add(normalized_url)

        return links

    def calculate_depth(self, url: str) -> int:
        """
        URLの階層深度を計算

        Args:
            url: 対象URL

        Returns:
            階層深度（base_urlを0とする）
        """
        url_path = urlparse(url).path.rstrip('/')

        # base_pathと比較
        if url_path == self.base_path:
            return 0

        # base_pathからの相対パスを取得
        if self.base_path and url_path.startswith(self.base_path + '/'):
            # base_pathが空でない場合
            relative_path = url_path[len(self.base_path):].strip('/')
        elif self.base_path == '' or self.base_path == '/':
            # base_pathが空またはルートの場合
            relative_path = url_path.strip('/')
        else:
            # base_pathがプレフィックスでない場合
            relative_path = url_path.strip('/')

        if not relative_path:
            return 0

        return len(relative_path.split('/'))

    def crawl(self, url: str, parent: str = None):
        """
        再帰的にページをクロール

        Args:
            url: クロールするURL
            parent: 親ページのURL
        """
        # URLを正規化
        url = self.normalize_url(url)

        # 訪問済みチェック
        if url in self.visited_urls:
            return

        # URLから深度を計算
        depth = self.calculate_depth(url)

        print(f"クロール中 (深度{depth}): {url}")

        # ページ取得
        html, title, final_url, error = self.get_page_content(url)
        if html is None:
            # 取得失敗した場合はfailed_urlsに記録
            self.failed_urls[url] = error or "不明なエラー"
            return

        # 成功した場合のみ訪問済みに追加
        self.visited_urls.add(url)

        # リダイレクトされた場合、最終URLを正規化して使用
        if final_url and final_url != url:
            final_url = self.normalize_url(final_url)
            # リダイレクト先も訪問済みに追加
            if final_url != url and final_url not in self.visited_urls:
                self.visited_urls.add(final_url)
                # 最終URLの深度も保存
                final_depth = self.calculate_depth(final_url)
                self.url_titles[final_url] = title
                self.url_depths[final_url] = final_depth

        # タイトルと深度を保存
        self.url_titles[url] = title
        self.url_depths[url] = depth

        # 親子関係を記録
        if parent:
            self.url_tree[parent].append({
                'url': url,
                'title': title,
                'depth': depth
            })
        else:
            # ルートページ
            self.url_tree['root'].append({
                'url': url,
                'title': title,
                'depth': depth
            })

        # リンク抽出
        links = self.extract_links(html, url)

        # 各リンクを再帰的にクロール
        for link in links:
            if link not in self.visited_urls:
                time.sleep(0.5)  # 負荷軽減のため少し待機
                self.crawl(link, url)

    def get_sorted_urls(self) -> List[Tuple[str, str, int]]:
        """
        全URLを取得し、五十音順でソート

        Returns:
            (URL, タイトル, 深度)のリスト
        """
        urls_data = []
        for url in self.visited_urls:
            title = self.url_titles.get(url, '')
            depth = self.url_depths.get(url, 0)
            urls_data.append((url, title, depth))

        # URLで五十音順ソート（日本語対応）
        urls_data.sort(key=lambda x: x[0])

        return urls_data

    def export_to_csv(self, filename: str):
        """
        CSV形式でエクスポート

        Args:
            filename: 出力ファイル名
        """
        urls_data = self.get_sorted_urls()

        with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)

            for url, title, depth in urls_data:
                # 階層に応じて列をずらす
                row = [''] * depth + [url, title]
                writer.writerow(row)

            # 取得失敗URLがある場合、最後に追加
            if self.failed_urls:
                # 空行を追加
                writer.writerow([])
                writer.writerow([])
                # セクションヘッダー
                writer.writerow(['【取得失敗URL一覧】'])
                writer.writerow(['URL', 'エラー理由'])

                # 失敗URLをソートして出力
                sorted_failed = sorted(self.failed_urls.items(), key=lambda x: x[0])
                for url, error in sorted_failed:
                    writer.writerow([url, error])

        print(f"CSV出力完了: {filename}")
        if self.failed_urls:
            print(f"  取得失敗URL: {len(self.failed_urls)}件")

    def export_to_html(self, filename: str):
        """
        HTML形式（アコーディオン付き）でエクスポート

        Args:
            filename: 出力ファイル名
        """
        urls_data = self.get_sorted_urls()

        # 階層構造を構築
        tree = self._build_tree(urls_data)

        html_content = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>サイトマップ - {self.base_url}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }}
        .tree {{
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .tree-item {{
            margin: 10px 0;
        }}
        .tree-content {{
            padding-left: 20px;
            border-left: 2px solid #e0e0e0;
            margin-left: 10px;
        }}
        .toggle {{
            cursor: pointer;
            user-select: none;
            padding: 8px;
            background-color: #f8f8f8;
            border-radius: 4px;
            display: inline-block;
            margin: 5px 0;
            transition: background-color 0.3s;
        }}
        .toggle:hover {{
            background-color: #e8e8e8;
        }}
        .toggle::before {{
            content: '▶ ';
            display: inline-block;
            transition: transform 0.3s;
        }}
        .toggle.open::before {{
            transform: rotate(90deg);
        }}
        .link {{
            color: #1976D2;
            text-decoration: none;
            font-weight: bold;
            font-size: 16px;
        }}
        .link:hover {{
            text-decoration: underline;
            color: #1565C0;
        }}
        .url {{
            color: #666;
            font-size: 12px;
            margin-top: 3px;
            word-break: break-all;
        }}
        .children {{
            display: none;
            margin-top: 5px;
        }}
        .children.show {{
            display: block;
        }}
        .no-children {{
            padding: 8px;
            margin: 5px 0;
        }}
        .failed-section {{
            margin-top: 40px;
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .failed-section h2 {{
            color: #d32f2f;
            border-bottom: 2px solid #d32f2f;
            padding-bottom: 10px;
            margin-top: 0;
        }}
        .failed-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        .failed-table th {{
            background-color: #f5f5f5;
            padding: 10px;
            text-align: left;
            border-bottom: 2px solid #ddd;
            font-weight: bold;
        }}
        .failed-table td {{
            padding: 10px;
            border-bottom: 1px solid #eee;
            word-break: break-all;
        }}
        .failed-table tr:hover {{
            background-color: #f9f9f9;
        }}
        .error-msg {{
            color: #d32f2f;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <h1>サイトマップ</h1>
    <p><strong>ベースURL:</strong> {self.base_url}</p>
    <p><strong>総ページ数:</strong> {len(urls_data)}</p>

    <div class="tree">
        {self._generate_tree_html(tree)}
    </div>

    {self._generate_failed_urls_html()}

    <script>
        document.querySelectorAll('.toggle').forEach(toggle => {{
            toggle.addEventListener('click', function(e) {{
                e.stopPropagation();
                this.classList.toggle('open');
                const children = this.parentElement.querySelector('.children');
                if (children) {{
                    children.classList.toggle('show');
                }}
            }});
        }});
    </script>
</body>
</html>
"""

        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"HTML出力完了: {filename}")
        if self.failed_urls:
            print(f"  取得失敗URL: {len(self.failed_urls)}件")

    def _generate_failed_urls_html(self) -> str:
        """
        取得失敗URL一覧のHTMLを生成

        Returns:
            HTML文字列
        """
        if not self.failed_urls:
            return ''

        sorted_failed = sorted(self.failed_urls.items(), key=lambda x: x[0])

        rows_html = ''
        for url, error in sorted_failed:
            rows_html += f'''
            <tr>
                <td><a href="{url}" target="_blank">{url}</a></td>
                <td class="error-msg">{error}</td>
            </tr>
            '''

        html = f'''
    <div class="failed-section">
        <h2>取得失敗URL一覧 ({len(self.failed_urls)}件)</h2>
        <table class="failed-table">
            <thead>
                <tr>
                    <th style="width: 60%;">URL</th>
                    <th style="width: 40%;">エラー理由</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
        '''

        return html

    def _build_tree(self, urls_data: List[Tuple[str, str, int]]) -> Dict:
        """
        URLデータから階層ツリーを構築

        Args:
            urls_data: (URL, タイトル, 深度)のリスト

        Returns:
            階層ツリー構造
        """
        tree = {'children': []}
        depth_map = {-1: tree}

        for url, title, depth in urls_data:
            node = {
                'url': url,
                'title': title or url,
                'depth': depth,
                'children': []
            }

            # 親ノードを探す
            parent_depth = depth - 1
            while parent_depth >= -1:
                if parent_depth in depth_map:
                    depth_map[parent_depth]['children'].append(node)
                    depth_map[depth] = node
                    break
                parent_depth -= 1

        return tree

    def _generate_tree_html(self, tree: Dict, level: int = 0) -> str:
        """
        ツリー構造からHTMLを生成

        Args:
            tree: ツリーノード
            level: 現在の階層レベル

        Returns:
            HTML文字列
        """
        html = ''

        for node in tree.get('children', []):
            has_children = len(node.get('children', [])) > 0

            if has_children:
                html += f'''
                <div class="tree-item">
                    <div class="toggle">
                        <a href="{node['url']}" class="link" target="_blank">{node['title']}</a>
                        <div class="url">{node['url']}</div>
                    </div>
                    <div class="children tree-content">
                        {self._generate_tree_html(node, level + 1)}
                    </div>
                </div>
                '''
            else:
                html += f'''
                <div class="tree-item no-children">
                    <a href="{node['url']}" class="link" target="_blank">{node['title']}</a>
                    <div class="url">{node['url']}</div>
                </div>
                '''

        return html

    def sanitize_filename(self, url: str) -> str:
        """
        URLからファイル名に使える文字列を生成

        Args:
            url: URL

        Returns:
            サニタイズされたファイル名
        """
        # プロトコルを除去
        name = re.sub(r'^https?://', '', url)
        # 記号を除去
        name = re.sub(r'[^\w\s-]', '', name)
        # 空白をアンダースコアに
        name = re.sub(r'\s+', '_', name)
        # 末尾のアンダースコアを除去
        name = name.strip('_')

        return name


def main():
    """メイン関数"""
    print("=== サイトマップクローラー ===\n")

    # URL入力
    base_url = input("クロールするURLを入力してください: ").strip()

    if not base_url:
        print("URLが入力されていません。")
        return

    # クローラー初期化
    crawler = SitemapCrawler(base_url)

    # クロール実行
    print(f"\nクロールを開始します: {crawler.base_url}\n")
    crawler.crawl(crawler.base_url)

    print(f"\n完了: {len(crawler.visited_urls)} ページを取得しました\n")

    # ファイル名生成
    base_filename = crawler.sanitize_filename(base_url)
    csv_filename = f"{base_filename}.csv"
    html_filename = f"{base_filename}.html"

    # エクスポート
    crawler.export_to_csv(csv_filename)
    crawler.export_to_html(html_filename)

    print("\n処理完了!")


if __name__ == "__main__":
    main()
