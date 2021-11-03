from azure.ai.formrecognizer import FormRecognizerClient
from azure.ai.formrecognizer import FormTrainingClient
from databricks.koalas import option_context
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import databricks.koalas as ks


class SelfSupervisedProcess:
    def __init__(self) -> None:
        pass

    def get_forms_id_models(self, configData: ks.DataFrame,
                            sasUrl: str,
                            formTrainingClient: FormTrainingClient,
                            formNameColumn: str = "nombre_forma",
                            storageNameColumn: str = "storage_name",
                            modelIdColumn: str = "model_id",
                            withLabels: bool = False,
                            timeOut: float = 600,
                            includeSubfolders: bool = False) -> dict:
        configData = configData.reset_index(drop=True)
        pollerDictionary = {}
        for index, _ in enumerate(configData[formNameColumn].to_numpy()):
            prefixForm = configData.loc[index, [storageNameColumn]].to_numpy()[0].strip()
            idValue = configData.loc[index, [modelIdColumn]].to_numpy()[0]
            if str(idValue) == "-1":
                try:
                  poller = formTrainingClient.begin_training(training_files_url= sasUrl, 
                                                          use_training_labels=withLabels,
                                                          model_name=f"{prefixForm}",
                                                          include_subfolders=includeSubfolders, prefix=prefixForm)
                  pollerResult = poller.result(timeout=timeOut)
                  pollerDictionary[f"{prefixForm}"] = pollerResult.model_id
                except Exception as exc:
                  print(f"Error en {prefixForm}: {exc}")
                  pollerDictionary[f"{prefixForm}"] = "NaN"
            else:
                pollerDictionary[f"{prefixForm}"] = configData.loc[index, [modelIdColumn]].to_numpy()[0] 
            
            
            
        return pollerDictionary  

    def get_model_forms_toprocess(self, todayForms: ks.DataFrame,
                                modelName: str,
                                directoryNameColumn: str = "directory_name",
                                filePathColumn: str = "file_path",
                                keyColumn: str = "key",
                                ) -> ks.DataFrame:
        
        modelForms = todayForms.loc[todayForms[directoryNameColumn] == modelName,
                                    [filePathColumn,  keyColumn, directoryNameColumn]]
        return modelForms

                    
    def create_forms_vlues_keys(self, pollerDict: dict,
                                toProcessForms: ks.DataFrame,
                                formsClient: FormRecognizerClient,
                                filePathColumn: str = "file_path",
                                directoryNameColumn: str = "directory_name",
                                keyColumn: str = "key",
                                timeOut: float = 60) -> ks.DataFrame:
        
        reconizerFormsToDay = ks.DataFrame(columns=["label", "value", "confidence", "model_name", "key"])
        for modelName in pollerDict:
            modelForms = self.get_model_forms_toprocess(toProcessForms, modelName)
            dictForsmResults = self.get_forms_reconizer_values(modelForms=modelForms, 
                                                               modelId=pollerDict.get(modelName), 
                                                               formsClient=formsClient,
                                                               filePathColumn=filePathColumn,
                                                               directoryNameColumn=directoryNameColumn,
                                                               keyColumn=keyColumn,
                                                               timeOut=timeOut)
            temporalDataFrame = ks.DataFrame(dictForsmResults)
            reconizerFormsToDay = reconizerFormsToDay.append(temporalDataFrame)
        
        return reconizerFormsToDay.drop_duplicates()



    def get_formmetadta_koala(self, queryMetaData: str,
                            spark: SparkSession,
                            ingestionDateColumn: str = "ingestion_date", 
                            deltaDays: int = 0):
        
        formsMetadata = spark.sql(queryMetaData)\
        .withColumn("diff_in_days", datediff(date_sub(current_date(), 0), col(ingestionDateColumn)))\
        .filter(col("diff_in_days")<= deltaDays)\
        .drop("diff_in_days")
        with option_context("compute.default_index_type", "distributed-sequence"):
            toProcessForms = formsMetadata.to_koalas()
            
        return toProcessForms
    
    def get_poller_form(self, formPath: str ,
                        modelId: str,
                        formsClient: FormRecognizerClient,
                        timeOut: float = 60):
        
        with open(formPath, "rb") as fp:
            poller = formsClient.begin_recognize_custom_forms(model_id=modelId,
                                                              form=fp,
                                                              include_field_elements= True,
                                                              content_type="application/pdf")
        poolerResult = poller.result(timeout=timeOut )
        print(f"{formPath} se abrió bien")
        return poolerResult
    
    
    def get_poller_content(self, poolerResult:list) -> object:
        confidenceList = []
        valueList = []
        labelList = []
        for recognized_form in poolerResult:
            for name, field in recognized_form.fields.items():
                value = str(field.value)
                confidence = field.confidence
                if field.label_data:
                    label = str(field.label_data.text)
                else:
                    label = str(name)
                
                labelList.append(label)
                valueList.append(value)
                confidenceList.append(confidence)
                    
        return labelList, valueList, confidenceList
    
    
    def get_label_value_confidence_formas(self,
                                      formPath: str,
                                      modelId: str,
                                      formsClient:FormRecognizerClient,
                                      timeOut: float) -> object:
        if modelId != "NaN":
            try:
                poolerResult = self.get_poller_form(formPath, modelId, formsClient, timeOut)
                labelList, valueList, confidenceList = self.get_poller_content(poolerResult)
                print(f"Exito para {modelId}: {formPath}")
                return labelList, valueList, confidenceList
                
            except Exception as esc:
                print(f"Error en {modelId}: {esc}" )
                return [None], [None], [None]
                     
        else:
            print(f"Fracaso para {modelId}: {formPath}")
            return [None], [None], [None]

    def get_forms_reconizer_values(self, 
                                   modelForms: ks.DataFrame,
                                   modelId: str,
                                   formsClient: FormRecognizerClient,
                                   filePathColumn: str = "file_path",
                                   directoryNameColumn: str = "directory_name",
                                   keyColumn: str = "key",
                                   timeOut: float = 60)->dict:
        resultsDict = {}        
        labelList = []
        valueList = []
        confidenceList = []
        modelNameList = []
        keylist = []
        modelIdList = []
        modelFormsReset = modelForms.reset_index()   
        for index, formPath in enumerate(modelFormsReset[filePathColumn].to_numpy()):
            documentKey = modelFormsReset.loc[index, [keyColumn]].values[0]
            modelName = modelFormsReset.loc[index, [directoryNameColumn]].values[0]
            label, value, confidence =self.get_label_value_confidence_formas(formPath, modelId, formsClient, timeOut)
            labelSize = len(label)
            modelNameList.extend([modelName] * labelSize)
            keylist.extend([documentKey] * labelSize)
            modelIdList.extend([modelId] * labelSize)
            labelList.extend(label)
            valueList.extend(value)
            confidenceList.extend(confidence)


            
        resultsDict["label"] = labelList
        resultsDict["value"] = valueList
        resultsDict["confidence"] = confidenceList
        resultsDict["model_name"] = modelNameList
        resultsDict["model_id"] = modelIdList
        resultsDict["key"] = keylist
        
        return resultsDict


