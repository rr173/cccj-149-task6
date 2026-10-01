"""启动信号监测服务：python -m pv_service.signals.serve --data data/signals --port 8090"""
import argparse

from .api import serve
from .service import SignalApplication


def main():
    ap = argparse.ArgumentParser(description="药物安全信号可复演窗口计算服务")
    ap.add_argument("--data", default="data/signals", help="数据目录")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--shard-size", type=int, default=50)
    ap.add_argument("--auto-worker", action="store_true",
                    help="后台自动续跑：恢复中断作业并推进 PENDING 作业")
    args = ap.parse_args()

    app = SignalApplication(args.data, shard_size=args.shard_size,
                            auto_worker=args.auto_worker)
    try:
        serve(app, args.host, args.port)
    finally:
        app.shutdown()


if __name__ == "__main__":
    main()
