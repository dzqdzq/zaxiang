import os
import sys
import argparse
import boto3
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import concurrent.futures
from threading import Lock
import time

region = "us-west-2"
bucket_name = "image-browser"
env_access_key_id = "oss_image_browser_access_key_id"
env_access_key_secret = "oss_image_browser_access_key_secret"


class AsyncS3Uploader:
    def __init__(self, workers: int = 10):
        """
        初始化异步S3上传器
        
        Args:
            workers: 最大并发工作线程数
        """
        session = boto3.Session(
            access_key_id=os.getenv(env_access_key_id),
            access_key_secret=os.getenv(env_access_key_secret)
            region_name=region
        )
        self.s3 = session.client('s3')
        self.workers = workers
        self.lock = Lock()
        self.uploaded_count = 0
        self.failed_count = 0
    
    def get_file_headers(self, file_path: str) -> Dict[str, str]:
        """根据文件类型获取合适的HTTP头"""
        headers = {
            'StorageClass': 'STANDARD'
        }
        
        # 根据文件扩展名设置Content-Type
        ext = Path(file_path).suffix.lower()
        content_types = {
            '.html': 'text/html',
            '.css': 'text/css',
            '.js': 'application/javascript',
            '.json': 'application/json',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.gif': 'image/gif',
            '.svg': 'image/svg+xml',
            '.ico': 'image/x-icon',
            '.webp': 'image/webp',
            '.txt': 'text/plain',
            '.md': 'text/markdown',
            '.xml': 'application/xml',
            '.pdf': 'application/pdf',
            '.zip': 'application/zip',
            '.woff': 'font/woff',
            '.woff2': 'font/woff2',
            '.ttf': 'font/ttf',
            '.eot': 'application/vnd.ms-fontobject'
        }
        
        if ext in content_types:
            headers['ContentType'] = content_types[ext]
        
        if Path(file_path).name == 'index.html':
            headers['CacheControl'] = 'no-cache'
        
        return headers
    
    def _upload_single_file(self, file_path: Path, s3_key: str) -> bool:
        """上传单个文件（线程安全）"""
        try:
            headers = self.get_file_headers(str(file_path))
            
            # 将 headers 转换为 S3 ExtraArgs 格式
            extra_args = {}
            if 'ContentType' in headers:
                extra_args['ContentType'] = headers['ContentType']
            if 'CacheControl' in headers:
                extra_args['CacheControl'] = headers['CacheControl']
            if 'StorageClass' in headers:
                extra_args['StorageClass'] = headers['StorageClass']
            
            self.s3.upload_file(
                str(file_path), 
                bucket_name, 
                s3_key,
                ExtraArgs=extra_args
            )
            
            with self.lock:
                self.uploaded_count += 1
                print(f"✅ 上传成功 ({self.uploaded_count}): {file_path.name} -> {s3_key}")
            
            return True
            
        except Exception as e:
            with self.lock:
                self.failed_count += 1
                print(f"❌ 上传失败 ({self.failed_count}): {file_path.name} -> {e}")
            return False
    
    def upload(self, src_path: str, dst_path: str, include_root: bool = False) -> bool:
        """
        上传文件或目录到S3
        
        Args:
            src_path: 本地源路径（文件或目录）
            dst_path: S3目标路径
            include_root: 
                - True: 整个目录上传到dst_path目录中 (类似 cp src dst)
                - False: 目录内容上传到dst_path目录中 (类似 cp -rf src/* dst)
        
        Returns:
            bool: 上传是否成功
        """
        src_path = Path(src_path)
        dst_path = dst_path.rstrip('/')
        
        if not src_path.exists():
            print(f"❌ 源路径不存在: {src_path}")
            return False
        
        start_time = time.time()
        
        try:
            if src_path.is_file():
                # 上传单个文件
                return self._upload_single_file_sync(src_path, dst_path)
            elif src_path.is_dir():
                # 上传目录
                return self._upload_directory(src_path, dst_path, include_root)
            else:
                print(f"❌ 不支持的路径类型: {src_path}")
                return False
                
        except Exception as e:
            print(f"❌ 上传过程中发生错误: {e}")
            return False
        finally:
            elapsed_time = time.time() - start_time
            print(f"⏱️  总耗时: {elapsed_time:.2f}秒")
    
    def _upload_single_file_sync(self, file_path: Path, dst_path: str) -> bool:
        """同步上传单个文件"""
        try:
            # 检查是否为 .DS_Store 文件
            if file_path.name == '.DS_Store':
                print(f"🚫 已排除 .DS_Store 文件: {file_path}")
                return False
            
            # 检查是否是以 . 开头的文件
            if file_path.name.startswith('.'):
                print(f"⚠️  警告: 正在上传以 . 开头的文件: {file_path}")
                print("   这个文件通常是隐藏文件，请确认是否需要上传")
            
            if dst_path is None:
                dst_path = ""
            
            if dst_path.endswith('/') or not dst_path:
                if dst_path:
                    s3_key = f"{dst_path}/{file_path.name}"
                else:
                    s3_key = file_path.name
            else:
                s3_key = dst_path
            
            s3_key = s3_key.lstrip('/')
            
            print(f"📤 上传文件: {file_path} -> {s3_key}")
            return self._upload_single_file(file_path, s3_key)
            
        except Exception as e:
            print(f"❌ 文件上传失败 {file_path}: {e}")
            return False
    
    def _upload_directory(self, dir_path: Path, dst_path: str, include_root: bool) -> bool:
        """上传目录"""
        print(f"📁 开始上传目录: {dir_path} -> {dst_path}")
        print(f"📋 模式: {'包含根目录' if include_root else '仅内容'}")
        print(f"🚀 并发上传 (最大 {self.workers} 线程)")
        
        # 收集所有需要上传的文件
        upload_tasks = []
        excluded_files = []
        warning_files = []
        
        for file_path in dir_path.rglob('*'):
            if file_path.is_file():
                # 排除 .DS_Store 文件
                if file_path.name == '.DS_Store':
                    excluded_files.append(file_path)
                    continue
                
                # 检查是否是以 . 开头的文件
                if file_path.name.startswith('.'):
                    warning_files.append(file_path)
                
                if include_root:
                    relative_path = str(file_path)
                else:
                    relative_path = file_path.relative_to(dir_path)
                
                relative_path = str(relative_path)
                s3_key = f"{dst_path}/{relative_path}".replace("\\", "/")
                s3_key = s3_key.lstrip('/')
                
                upload_tasks.append((file_path, s3_key))
        
        # 显示排除的文件
        if excluded_files:
            print(f"🚫 已排除 {len(excluded_files)} 个 .DS_Store 文件")
        
        # 显示警告信息
        if warning_files:
            print(f"⚠️  发现 {len(warning_files)} 个以 . 开头的文件:")
            for file_path in warning_files:
                print(f"   ⚠️  {file_path}")
            print("   这些文件通常是隐藏文件，请确认是否需要上传")
        
        total_files = len(upload_tasks)
        print(f"📊 发现 {total_files} 个文件需要上传")
        
        if total_files == 0:
            print("ℹ️  没有文件需要上传")
            return True
        
        if total_files == 1:
            # 单个文件直接上传
            file_path, s3_key = upload_tasks[0]
            return self._upload_single_file(file_path, s3_key)
        else:
            # 多个文件使用并发上传
            return self._upload_concurrent(upload_tasks)
    
    def _upload_concurrent(self, upload_tasks: List[Tuple[Path, str]]) -> bool:
        """并发上传文件"""
        print("⚡ 使用并发上传模式")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
            # 提交所有上传任务
            future_to_task = {
                executor.submit(self._upload_single_file, file_path, s3_key): (file_path, s3_key)
                for file_path, s3_key in upload_tasks
            }
            
            # 等待所有任务完成
            for future in concurrent.futures.as_completed(future_to_task):
                file_path, s3_key = future_to_task[future]
                try:
                    future.result()  # 获取结果，如果有异常会在这里抛出
                except Exception as e:
                    print(f"❌ 任务异常 {file_path}: {e}")
        
        print(f"📊 并发上传完成: 成功 {self.uploaded_count} 个文件，失败 {self.failed_count} 个文件")
        return self.failed_count == 0


def upload(src_path: str, dst_path: str, include_root: bool = False, workers: int = 10) -> bool:
    """
    便捷的上传函数
    
    Args:
        src_path: 本地源路径
        dst_path: S3目标路径
        include_root: 是否包含根目录
        workers: 最大并发工作线程数
    
    Returns:
        bool: 上传是否成功
    """
    uploader = AsyncS3Uploader(workers=workers)
    return uploader.upload(src_path, dst_path, include_root)


# 保持向后兼容的简单上传函数
def upload_simple(local_directory, s3_file_path):
    """简单的单文件上传函数（保持向后兼容）"""
    session = boto3.Session(
       aws_access_key_id=os.getenv(env_access_key_id),
       aws_secret_access_key=os.getenv(env_access_key_secret),
       region_name=region
    )
    s3 = session.client('s3')
    print('上传S3参数:', local_directory, bucket_name, s3_file_path)
    s3.upload_file(local_directory, bucket_name, s3_file_path)


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='上传文件或目录到AWS S3',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python aws_s3_upload.py src                   # 上传src目录到S3根目录
  python aws_s3_upload.py src /                 # 上传src目录到S3根目录
  python aws_s3_upload.py src /images           # 上传src目录到S3的/images目录
  python aws_s3_upload.py dist /v1.0.0          # 上传dist目录到S3的/v1.0.0目录
  python aws_s3_upload.py file.txt /docs         # 上传单个文件到S3的/docs目录
        """
    )
    
    parser.add_argument(
        'src_path',
        help='本地源路径（文件或目录）'
    )
    
    parser.add_argument(
        'dst_path',
        nargs='?',
        default='/',
        help='S3目标路径（默认为根目录 /）'
    )
    
    parser.add_argument(
        '--include-root',
        action='store_true',
        help='包含根目录（类似 cp src dst，默认为 cp -rf src/* dst）'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='并发工作线程数（默认10）'
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    
    print(f"🚀 开始上传")
    print(f"📁 源路径: {args.src_path}")
    print(f"🎯 目标路径: {args.dst_path}")
    print(f"📋 模式: {'包含根目录' if args.include_root else '仅内容'}")
    print(f"⚡ 并发线程: {args.workers}")
    print("-" * 50)
    
    success = upload(args.src_path, args.dst_path, args.include_root, args.workers)
    
    if success:
        print("✅ 上传完成！")
        sys.exit(0)
    else:
        print("❌ 上传失败！")
        sys.exit(1)