import argparse
import csv
import json
import logging
import os
import re
from datetime import datetime, timedelta

from xopen import xopen

logger = logging.getLogger()
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
file_handler = logging.FileHandler(filename='cdr_parser.log')
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)


def argument_parser():
    parser = argparse.ArgumentParser(description='script for parsing stp cdr')
    parser.add_argument(
        '--file-mask',
        help='file name mask to filter files',
        dest='file_mask',
        default='dmp_json_ss7-{0}'.format((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')))
    parser.add_argument('--link', help='link id', nargs='*', type=int)
    parser.add_argument('--map-code', help='MAP code.', nargs='*', dest='map_code', type=int)
    parser.add_argument('--dst-gt', help='destination GT.', nargs='*', dest='dst_gt', type=str)
    parser.add_argument('--src-gt', help='source GT.', nargs='*', dest='src_gt', type=str)
    parser.add_argument(
        '--file-path',
        help='path to folder with CDR files',
        dest='file_path',
        required=True)
    parser.add_argument(
        '--output-file',
        help='output file name(YYYY-MM-DD.csv postfix will add by default if --file-postfix did not specify)',
        required=True,
        dest='output_file')
    parser.add_argument('--file-postfix', dest='file_postfix', help='add postfix to output file name')
    return parser.parse_args()


class CdrParser:

    def __init__(self,
                 file_mask,
                 file_path,
                 output_file,
                 link=None,
                 map_code=None,
                 dst_gt=None,
                 src_gt=None,
                 file_postfix=None):
        self.file_mask = file_mask
        self.file_path = file_path
        self.link = link
        self.map_code = map_code
        self.dst_gt = dst_gt
        self.src_gt = src_gt
        self.cdr_files = sorted(list(
            map(
                lambda x: os.path.join(self.file_path, x),
                filter(lambda x: self.file_mask in x, os.listdir(self.file_path)))))
        self.cdr = []
        if file_postfix:
            self.output_file = '{0}-{1}'.format(output_file, file_postfix)
        else:
            self.output_file = '{0}-{1}.csv'.format(output_file, (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))
        logger.info('link filter: {0}'.format(self.link))
        logger.info('map_code filter: {0}'.format(self.map_code))
        logger.info('dst_gt filter: {0}'.format(self.dst_gt))
        logger.info('src_gt filter: {0}'.format(self.src_gt))
        logger.info('cdr files to proceed: {0}'.format(self.cdr_files))

    def parse_file(self):
        for zip_file in self.cdr_files:
            logger.info('start parse file: {0}'.format(zip_file))
            logger.debug('start open gzip file')
            f_gz = xopen(zip_file)
            logger.debug('finished open gzip file')
            start_parsing_time = datetime.now()
            for i, cdr_record in enumerate(f_gz):
                try:
                    cdr_obj = json.loads(cdr_record)
                except ValueError:
                    cdr_record = cdr_record.replace('."', '.')
                    cdr_record = re.sub(r'(:"\d*\.\d*\.\w*)""', r'\g<1>"', cdr_record)
                    cdr_obj = json.loads(cdr_record)
                cdr = self.parse_cdr_rec(cdr_obj)
                for record in cdr:
                    if self.filter_cdr(record):
                        self.cdr.append(record)
                if i % 1000 == 0:
                    parsing_duration = datetime.now() - start_parsing_time
                    parsing_duration_sec = '{0}.{1}'.format(parsing_duration.seconds, parsing_duration.microseconds)
                    logger.debug('1000 rows parsed in {1} seconds'.format(i, parsing_duration_sec))
                    start_parsing_time = datetime.now()
            f_gz.close()
            logger.info('finished parse file: {0}'.format(zip_file))

    def print_cdr(self):
        print('\n'.join(str(cdr) for cdr in self.cdr))

    def save_to_file(self):
        logger.debug('start save cdr to file')
        with open(self.output_file, 'w', 0) as csv_f:
            csv_writer = csv.writer(csv_f, delimiter=';')
            csv_writer.writerow(['TS', 'MAP_CODE', 'DTID', 'OTID', 'SCCP_A', 'SCCP_B', 'SMS_A', 'SMS_B', 'IMSI_A'])
            for cdr in self.cdr:
                try:
                    csv_writer.writerow(
                        [cdr['TS'], cdr['TCAP']['MAP'], cdr['TCAP']['DST'], cdr['TCAP']['SRC'],
                         cdr['SCCP']['A'], cdr['SCCP']['B'], cdr['SMS']['A'], cdr['SMS']['B'], cdr['SMS']['IMSI_A']])
                except KeyError:
                    pass
        logger.debug('finished save cdr to file')

    def filter_cdr(self, cdr):
        if not any((self.link, self.map_code, self.dst_gt)):
            return True
        link_filter = True
        map_code_filter = True
        dst_gt_filter = True
        src_gt_filter = True
        try:
            if self.link:
                link_filter = cdr['GENERAL_PART']['LINK'] in self.link
            if self.map_code:
                map_code_filter = cdr['TCAP']['MAP'] in self.map_code
            if self.dst_gt:
                dst_gt_filter = any(map(lambda x: x in cdr['SCCP']['B'], self.dst_gt))
            if self.src_gt:
                src_gt_filter = any(map(lambda x: x in cdr['SCCP']['A'], self.src_gt))
        except KeyError:
            return False
        return all((link_filter, map_code_filter, dst_gt_filter, src_gt_filter))

    def parse_cdr_rec(self, cdr_rec):
        records = []
        for cdr_part in cdr_rec:
            cdr_dir = cdr_part.get('DIR')
            if cdr_dir is not None:
                rec = {'TS': datetime.fromtimestamp(float(cdr_part['TS'])).strftime('%Y.%m.%d %H:%M:%S.%f'),
                       'DIR': cdr_dir}
                records.append(rec)
            elif 'MTP' in cdr_part:
                rec['MTP'] = cdr_part['MTP']
            elif 'SCCP' in cdr_part:
                rec['SCCP'] = cdr_part['SCCP']
            elif 'TCAP' in cdr_part:
                rec['TCAP'] = cdr_part['TCAP']
            elif 'SMS' in cdr_part:
                rec['SMS'] = cdr_part['SMS']
        return records


if __name__ == '__main__':
    logger.info('start work')
    args = argument_parser()
    logger.debug('provided arguments\n{0}'.format(args))
    try:
        cdr_parser = CdrParser(
            args.file_mask,
            args.file_path, args.output_file, args.link, args.map_code, args.dst_gt, args.src_gt, args.file_postfix)
        cdr_parser.parse_file()
        cdr_parser.save_to_file()
    except:
        logger.exception('ann exception during parsing')
    logger.info('finished work')
