from azure.ai.textanalytics import TextAnalyticsClient
import databricks.koalas as ks

class NerLinesProcess:

    def __init__(self) -> None:
        pass

    def get_entities_lines_dict(self, documentLines: ks.DataFrame,
                                    textAnalyzerClient: TextAnalyticsClient,
                                    keyColumnName: str = "key",
                                    textLineColumn: str = "text_lines",
                                    lineIdColumn: str = "lines_id") -> dict:

        textList = []
        categoryList = []
        subcategoriesList = []
        confidenceList = []
        lineIdList = []
        keyList = []
        for idx, text in enumerate(documentLines[textLineColumn].to_numpy()):
            result = textAnalyzerClient.recognize_entities([text], language="es")[0]
            if result.entities:
                for entity  in result.entities:
                    textList.append(entity.text)
                    categoryList.append(entity.category)
                    subcategoriesList.append(entity.subcategory)
                    confidenceList.append(entity.confidence_score)
                    lineIdList.append(documentLines.loc[idx, [lineIdColumn]][0])
                    keyList.append(documentLines.loc[idx, [keyColumnName]][0])
            else:
                textList.append(None)
                categoryList.append(None)
                confidenceList.append(None)
                subcategoriesList.append(None)
                lineIdList.append(documentLines.loc[idx, [lineIdColumn]][0])
                keyList.append(documentLines.loc[idx, [keyColumnName]][0])
        
        return {"lines_id": lineIdList,
                "key": keyList,
                "detected_text": textList,
                "category": categoryList,
                "sub_caregory": subcategoriesList,
                "confidence_score": confidenceList}
            
        
    def merge_ner__lines_data(self, documentLines: ks.DataFrame, 
                            textAnalyzerClient: TextAnalyticsClient,
                            keyColumnName: str = "key",
                            textLineColumn: str = "text_lines",
                            lineIdColumn: str = "lines_id"):
        entitiesDatadict = self.get_entities_lines_dict(documentLines, textAnalyzerClient,
                                            keyColumnName, textLineColumn, lineIdColumn)
        entitiesData = ks.DataFrame(entitiesDatadict)
        finalTable = documentLines.merge(entitiesData, on=[keyColumnName, lineIdColumn], suffixes=["r_", "l_"])
        
        return finalTable