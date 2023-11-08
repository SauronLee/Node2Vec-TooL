# -*- coding: utf-8 -*-            
# @Author : lixiaoran
# @Time : 2023/11/8 14:37

import logging
import os
import json
import pandas as pd
import argparse
from collections import Counter
from nltk.corpus import stopwords
from tqdm import tqdm



parser = argparse.ArgumentParser()
parser.add_argument("--data_path", default='/data/lxr/WitskyProjects/ToolBench/data/retrieval/G1/', type=str,
                    required=False,
                    help="The input data dir. Should contain the .tsv files for the task.")
args = parser.parse_args()

"""
6	
"{
""category_name"": ""Logistics"", 
""tool_name"": ""Transportistas de Argentina"", 
""api_name"": ""/cities/search/:stateIsoCode/:keyword"", 
""api_description"": ""Search city for iso state and keyword name."", 
""required_parameters"": [{""name"": ""stateIsoCode"", ""type"": ""STRING"", ""description"": ""State ISO Code"", ""default"": """"}, {""name"": ""keyword"", ""type"": ""STRING"", ""description"": ""Keyword to search, example: Caballito"", ""default"": """"}], ""optional_parameters"": [], ""method"": ""GET"", ""template_response"": {""statusCode"": ""int"", ""message"": ""str"", ""error"": ""str""}}"
"""

class ToolApiProcessor:
    def __init__(self, config):
        self.config = config
        self.ir_corpus = self.loading_data()
        self.ir_corpus = self.clean_data()

    def loading_data(self):
        documents_df = pd.read_csv(os.path.join(self.config.data_path, 'corpus.tsv'), sep='\t')
        ir_corpus = self._process_retrieval_ducoment(documents_df)
        return ir_corpus

    def _process_retrieval_ducoment(self, documents_df):
        ir_corpus = {}
        print("raw data processing...")
        for row in tqdm(documents_df.itertuples()):
            doc = json.loads(row.document_content)
            ir_corpus[row.docid] = [(doc.get('category_name', '') or ''),
                                    (doc.get('tool_name', '') or ''),
                                    (doc.get('api_name', '') or ''),
                                    (doc.get('api_description', '') or '')]
        return ir_corpus

    def clean_data(self):
        processed_ir_corpus = {}

        stop_words = stopwords.words('english')
        stop_words.append('')

        all_words = []
        print("clean data processing...")
        for k, v in tqdm(self.ir_corpus.items()):
            current_v_list = []
            for i in v:
                i = i.replace('/', ' ')
                i = i.replace(':', ' ')
                i = i.lower()
                for j in i.split():
                    """remove stop words"""
                    if j in stop_words: continue
                    all_words.append(j)
                    current_v_list.append(j)

            processed_ir_corpus[k] = current_v_list
        wordcount = Counter(all_words)
        wordcount = [i for (i, _) in wordcount.most_common()]
        print(f"wordcount length {len(wordcount)}")
        # ['get', 'data', 'api', 'list', '-', 'search', 'endpoint', 'returns', 'finance', 'sports', 'id', 'information', 'social', 'specific', "btc',", 'user', 'news', 'return', 'given', 'location']
        # wordcount = wordcount[:20]
        usable_data = wordcount[round(0.001*len(wordcount)):][:round(0.001*len(wordcount))]

        cleaned_ir_corpus = {}
        for k, v in processed_ir_corpus.items():
            current_v_list = []
            for i in v:
                if i in usable_data:
                    current_v_list.append(i)

            if len(set(current_v_list)) == 0:
                print("error of empty")
                continue
            cleaned_ir_corpus[k] = set(current_v_list)
        return cleaned_ir_corpus









def main():
    ToolApiProcessor(args)


if __name__ == "__main__":
    main()
