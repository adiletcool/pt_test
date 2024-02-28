import argparse
import os
from dataclasses import dataclass

import psutil

from src.model import FilterQuery


@dataclass
class Config:
    path: str
    url: str
    filter_query: FilterQuery | None
    limit: int
    output: str
    cpu_count: int
    ram: int

    def __init__(self):
        # total_mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
        # total_mem_mb = int(total_mem_bytes / (1024. ** 2))
        free_memory_mb = psutil.virtual_memory().available // 1024 ** 2

        cli = argparse.ArgumentParser()
        cli.add_argument('-p', '--path', required=True, help='Path to input file', type=str)
        cli.add_argument('-u', '--url', required=True, help='URL to service', type=str)
        cli.add_argument('-f', '--filter', required=False, help='Filter lines. Format: values.d.j==90', type=str)
        cli.add_argument('-l', '--limit', required=False, help='Maximum lines per request. Default: 1', type=int, default=1)
        cli.add_argument('-o', '--output', required=False, help='Output file with ignored lines', type=str, default='output.txt')
        cli.add_argument('--vcpu', required=False, help='cpu count', type=int, default=os.cpu_count())
        cli.add_argument('--ram', required=False, help='RAM in MB', type=int, default=free_memory_mb)

        self.parse_args(cli)

    def parse_args(self, cli: argparse.ArgumentParser):
        args = cli.parse_args()
        self.path = args.path
        self.url = args.url
        self.limit = args.limit
        self.output = args.output
        self.cpu_count = args.vcpu
        self.ram = args.ram
        self.filter_query = None

        if args.filter is not None:
            assert '==' in args.filter
            filter_query = args.filter.split('==')
            assert len(filter_query) == 2
            self.filter_query = FilterQuery.from_query(args.filter)
