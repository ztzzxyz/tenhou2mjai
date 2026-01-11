import gzip
import shutil

import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import json


class MJDataDownloader:
    def __init__(self, base_url="https://storage.googleapis.com/mjlog/games",
                 max_workers=5, retry_times=3):
        self.base_url = base_url
        self.max_workers = max_workers
        self.retry_times = retry_times
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def check_url_exists(self, url):
        """检查URL是否存在"""
        for i in range(self.retry_times):
            try:
                response = self.session.head(url, timeout=10)
                if response.status_code == 200:
                    return True
                elif response.status_code == 404:
                    return False
            except requests.RequestException:
                if i == self.retry_times - 1:
                    return False
                time.sleep(1)
        return False

    def download_file(self, url, save_path):
        """下载单个文件"""
        for i in range(self.retry_times):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()

                # 创建保存目录
                os.makedirs(os.path.dirname(save_path), exist_ok=True)

                with open(save_path, 'wb') as f:
                    f.write(response.content)

                json_gz_path = os.path.join(os.path.dirname(save_path), os.path.basename(save_path) + ".gz")
                with open(save_path, 'rb') as f_in:
                    with gzip.open(json_gz_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(save_path)  # 删除原始JSON文件
                return True, f"下载成功: {json_gz_path}"
            except requests.RequestException as e:
                if i == self.retry_times - 1:
                    return False, f"下载失败 {url}: {str(e)}"
                time.sleep(2)
        return False, f"下载失败: {url}"

    def find_max_rounds(self, game_id):
        """查找某个比赛的最大轮数"""
        print(f"正在探测比赛 {game_id} 的轮数...")
        return 250

    def find_max_games(self):
        """查找最大比赛次数"""
        print("正在探测最大比赛次数...")

        low, high = 1, 1
        step = 100

        # 快速找到上限
        while True:
            test_url = f"{self.base_url}/{high}/1_0_mjai.json"
            if self.check_url_exists(test_url):
                low = high
                high += step
            else:
                break

        # 二分查找精确的最大比赛次数
        last_valid = low
        while low <= high:
            mid = (low + high) // 2
            test_url = f"{self.base_url}/{mid}/1_0_mjai.json"

            if self.check_url_exists(test_url):
                last_valid = mid
                low = mid + 1
            else:
                high = mid - 1

        return last_valid

    def download_game(self, game_id, max_rounds=None, save_dir="mjai_data"):
        """下载单场比赛的所有轮数"""
        if max_rounds is None:
            max_rounds = self.find_max_rounds(game_id)

        print(f"比赛 {game_id}: 发现 {max_rounds} 轮数据")

        tasks = []
        for round_id in range(1, max_rounds + 1):
            url = f"{self.base_url}/{game_id}/{round_id}_0_mjai.json"
            # 修改保存路径为 save_dir/game_id/文件名
            save_path = os.path.join(save_dir, str(game_id), f"{round_id}_0_mjai.json")

            tasks.append((url, save_path))

        # 并行下载
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {
                executor.submit(self.download_file, url, save_path): (url, save_path)
                for url, save_path in tasks
            }

            for future in as_completed(future_to_url):
                url, save_path = future_to_url[future]
                success, message = future.result()
                results.append((success, message))

                if success:
                    print(f"  ✓ {os.path.basename(save_path)}")

        success_count = sum(1 for success, _ in results if success)
        print(f"比赛 {game_id}: 完成 {success_count}/{len(tasks)} 个文件下载")
        return success_count, len(tasks)

    def download_range(self, start_game=1, end_game=None,
                       start_round=1, end_round=None,
                       save_dir="mjai_data"):
        """下载指定范围内的比赛数据"""

        if end_game is None:
            end_game = self.find_max_games()

        print(f"开始下载比赛 {start_game} 到 {end_game} 的数据")
        print(f"保存目录: {save_dir}")
        print(f"目录结构: {save_dir}/比赛次数/文件名")

        total_downloaded = 0
        total_files = 0

        for game_id in range(start_game, end_game + 1):
            print(f"\n{'=' * 50}")
            print(f"处理比赛 {game_id}/{end_game}")

            # 检查第一轮是否存在
            first_round_url = f"{self.base_url}/{game_id}/1_0_mjai.json"
            if not self.check_url_exists(first_round_url):
                print(f"比赛 {game_id} 不存在，跳过")
                continue

            # 下载该场比赛
            if start_round > 1 or end_round is not None:
                # 如果指定了轮数范围
                if end_round is None:
                    max_rounds = self.find_max_rounds(game_id)
                    end_round_actual = max_rounds
                else:
                    end_round_actual = end_round

                tasks = []
                for round_id in range(start_round, min(end_round_actual, self.find_max_rounds(game_id)) + 1):
                    url = f"{self.base_url}/{game_id}/{round_id}_0_mjai.json"
                    # 修改保存路径为 save_dir/game_id/文件名
                    save_path = os.path.join(save_dir, str(game_id), f"{round_id}_0_mjai.json")
                    tasks.append((url, save_path))

                # 并行下载指定轮数
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_url = {
                        executor.submit(self.download_file, url, save_path): (url, save_path)
                        for url, save_path in tasks
                    }

                    for future in as_completed(future_to_url):
                        url, save_path = future_to_url[future]
                        success, message = future.result()
                        if success:
                            print(f"  ✓ {os.path.basename(save_path)}")
                            total_downloaded += 1
                        total_files += 1
            else:
                # 下载所有轮数
                downloaded, total = self.download_game(game_id, save_dir=save_dir)
                total_downloaded += downloaded
                total_files += total

            # 短暂休眠避免请求过快
            time.sleep(0.5)

        print(f"\n{'=' * 50}")
        print(f"下载完成!")
        print(f"总计: {total_downloaded}/{total_files} 个文件下载成功")

        # 生成索引文件
        self.generate_index(save_dir)

    def generate_index(self, save_dir):
        """生成索引文件，记录已下载的数据"""
        index = {}

        for game_dir in os.listdir(save_dir):
            game_path = os.path.join(save_dir, game_dir)
            if os.path.isdir(game_path):
                # 尝试将目录名转换为比赛ID
                try:
                    game_id = int(game_dir)
                except ValueError:
                    # 如果不是数字目录，跳过
                    continue

                rounds = []

                for file in os.listdir(game_path):
                    if file.endswith(".json") and "_0_mjai.json" in file:
                        try:
                            # 提取轮数ID
                            round_id = int(file.split("_")[0])
                            rounds.append(round_id)
                        except ValueError:
                            continue

                if rounds:
                    index[game_id] = {
                        "total_rounds": len(rounds),
                        "min_round": min(rounds),
                        "max_round": max(rounds),
                        "rounds": sorted(rounds),
                        "directory": game_dir
                    }

        index_path = os.path.join(save_dir, "index.json")
        with open(index_path, 'w', encoding='utf-8') as f:
            json.dump(index, f, indent=2, ensure_ascii=False)

        print(f"索引文件已生成: {index_path}")
        print(f"已下载 {len(index)} 场比赛数据")
        return index

    def check_existing_files(self, save_dir="mjai_data"):
        """检查已下载的文件"""
        if not os.path.exists(save_dir):
            print(f"目录 {save_dir} 不存在")
            return {}

        existing = {}
        for game_dir in os.listdir(save_dir):
            game_path = os.path.join(save_dir, game_dir)
            if os.path.isdir(game_path):
                try:
                    game_id = int(game_dir)
                except ValueError:
                    continue

                files = []
                for file in os.listdir(game_path):
                    if file.endswith(".json") and "_0_mjai.json" in file:
                        files.append(file)

                if files:
                    existing[game_id] = len(files)

        return existing


def main():
    parser = argparse.ArgumentParser(description='下载MJAI比赛数据')
    parser.add_argument('--start', type=int, default=1, help='起始比赛ID')
    parser.add_argument('--end', type=int, default=None, help='结束比赛ID')
    parser.add_argument('--start-round', type=int, default=1, help='起始轮数')
    parser.add_argument('--end-round', type=int, default=None, help='结束轮数')
    parser.add_argument('--save-dir', type=str, default="mjai_data", help='保存目录')
    parser.add_argument('--threads', type=int, default=5, help='下载线程数')
    parser.add_argument('--check-only', action='store_true', help='仅检查不下载')
    parser.add_argument('--skip-existing', action='store_true', help='跳过已存在的文件')
    parser.add_argument('--list-games', action='store_true', help='列出已下载的比赛')

    args = parser.parse_args()

    downloader = MJDataDownloader(max_workers=args.threads)

    if args.list_games:
        # 列出已下载的比赛
        existing = downloader.check_existing_files(args.save_dir)
        if existing:
            print(f"已下载的比赛 ({len(existing)}场):")
            for game_id in sorted(existing.keys()):
                print(f"  比赛 {game_id}: {existing[game_id]} 个文件")
        else:
            print(f"目录 {args.save_dir} 中没有找到已下载的比赛数据")
        return

    if args.check_only:
        # 仅检查模式
        max_games = downloader.find_max_games()
        print(f"\n发现 {max_games} 场比赛")

        existing = downloader.check_existing_files(args.save_dir)

        for game_id in range(args.start, min(max_games, args.end if args.end else max_games) + 1):
            max_rounds = downloader.find_max_rounds(game_id)
            existing_count = existing.get(game_id, 0)
            status = "✓" if existing_count >= max_rounds else f"{existing_count}/{max_rounds}"
            print(f"比赛 {game_id}: {max_rounds} 轮 [{status}]")
    else:
        # 下载模式
        # 检查已存在的文件
        existing = {}
        if args.skip_existing:
            existing = downloader.check_existing_files(args.save_dir)
            print(f"发现 {len(existing)} 场已下载的比赛")

        downloader.download_range(
            start_game=args.start,
            end_game=args.end,
            start_round=args.start_round,
            end_round=args.end_round,
            save_dir=args.save_dir
        )


if __name__ == "__main__":
    main()
