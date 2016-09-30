""" Simple wordcount using map reduce """
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import json
import os

class ProcessChatLogs(luigi.Task):
    file_name = luigi.Parameter()

    def input(self):
        return luigi.contrib.hdfs.HdfsTarget(self.file_name)

    def run(self):
        with self.output().open('w') as output_file:
            for msg_dict in json.load(self.input().open('r')):
                output_file.write(msg_dict.get('message') + '\n')

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
            self.file_name.replace('.json', '_messages_only.txt'))


class ChatWordCount(luigi.contrib.hadoop.JobTask):
    file_name = luigi.Parameter()

    def output(self):
        file_dir = os.path.dirname(self.file_name)
        new_file_name = 'wordcount_{}'.format(
            os.path.basename(self.file_name).replace('.json', '.txt'))
        return luigi.contrib.hdfs.HdfsTarget(
            os.path.join(file_dir, new_file_name))

    def mapper(self, line):
        for word in line.strip().split():
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)

    def requires(self):
        return ProcessChatLogs(self.file_name)
