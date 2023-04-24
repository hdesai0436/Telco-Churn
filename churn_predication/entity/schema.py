import os
import sys
from pyspark.sql.types import IntegerType, TimestampType, StringType, FloatType, StructType, StructField
from typing import List
class ChurnDataSchema:
    def __init__(self):
        self.CustomerID: str = 'CustomerID'
        self.Count: str = 'Count'
        self.Country: str = 'Country'
        self.State: str= 'State'
        self.City: str = 'City'
        self.Zip_Code: str = 'Zip Code'
        self.Lat_Long: str = 'Lat Long'
        self.Latitude: str = 'Latitude'
        self.Longitude: str = 'Longitude'
        self.Gender: str = 'Gender'
        self.Senior_Citizen: str = 'Senior Citizen'
        self.Partner: str = 'Partner'
        self.Dependents: str ='Dependents'
        self.Tenure_Months:str ='Tenure Months'
        self.Phone_Service:str ='Phone Service'
        self.Multiple_Lines: str = 'Multiple Lines'
        self.Internet_Service:str ='Internet Service'
        self.Online_Security:str ='Online Security'
        self.Online_Backup: str='Online Backup'
        self.Device_Protection: str = 'Device Protection'
        self.Tech_Support: str = 'Tech Support'
        self.Streaming_TV: str = 'Streaming TV'
        self.Streaming_Movies: str = 'Streaming Movies'
        self.Contract: str = 'Contract'
        self.Paperless_Billing: str = 'Paperless Billing'
        self.Payment_Method: str = 'Payment Method'
        self.Monthly_Charges: str = 'Monthly Charges'
        self.Total_Charges: str = 'Total Charges'
        self.Churn_Label: str = 'Churn Label'
        self.Churn_Value: str = 'Churn Value'
        self.Churn_Score: str = 'Churn Score'
        self.CLTV: str = 'CLTV'
        self.Churn_Reason: str = 'Churn Reason'

    
    def dataframe_schema(self) -> StructType:
        try:
            schema = StructType([
                 StructField(self.CustomerID, StringType()),
                 StructField(self.Count, IntegerType()),
                 StructField(self.Country,StringType()),
                 StructField(self.State,StringType()),
                 StructField(self.City,StringType()),
                 StructField(self.Zip_Code,IntegerType()),
                 StructField(self.Lat_Long,FloatType()),
                 StructField(self.Latitude,FloatType()),
                 StructField(self.Longitude,FloatType()),
                 StructField(self.Gender, StringType()),
                 StructField(self.Senior_Citizen, StringType()),
                 StructField(self.Partner, StringType()),
                 StructField(self.Dependents, StringType()),
                 StructField(self.Tenure_Months, IntegerType()),
                 StructField(self.Phone_Service, StringType()),
                StructField(self.Multiple_Lines, StringType()),
                StructField(self.Internet_Service, StringType()),
                StructField(self.Online_Security, StringType()),
                StructField(self.Online_Backup, StringType()),
                StructField(self.Device_Protection, StringType()),
                StructField(self.Tech_Support, StringType()),
                StructField(self.Streaming_TV, StringType()),
                StructField(self.Streaming_Movies, StringType()),
                StructField(self.Contract, StringType()),
                StructField(self.Paperless_Billing, StringType()),
                StructField(self.Payment_Method, StringType()),
                StructField(self.Monthly_Charges, FloatType()),
                StructField(self.Total_Charges, FloatType()),
                StructField(self.Churn_Label, StringType()),
                 StructField(self.Churn_Value, IntegerType()),
                  StructField(self.Churn_Score, IntegerType()),
                   StructField(self.CLTV, IntegerType()),
                    StructField(self.Churn_Reason, StringType())
                ])
            return schema
        except Exception as e:
            raise(e)
        
    
    def target_column(self) -> str:
        return self.self.Churn_Label


    
    def one_hot_encoding_features(self) -> List[str]:
        feature = [
            self.Gender,
            self.Senior_Citizen,
            self.Partner,
            self.Dependents,
            self.Phone_Service,
            self.Multiple_Lines,
            self.Internet_Service,
            self.Online_Security,
            self.Online_Backup,
            self.Device_Protection,
            self.Tech_Support,
            self.Streaming_TV,
            self.Streaming_Movies,
            self.Contract,
            self.Paperless_Billing,
            self.Payment_Method,
            

        ]
        return feature
    
    def non_one_hont_encoder(self) -> List[str]:
        no_feature = [
            self.Tenure_Months,
            self.Monthly_Charges,
            self.Total_Charges
        ]
        return no_feature
    
    @property
    def im_one_hot_encoding_features(self) -> List[str]:
        return [f"im_{col}" for col in self.one_hot_encoding_features]

    @property
    def string_indexer_one_hot_features(self) -> List[str]:
        return [f"si_{col}" for col in self.one_hot_encoding_features]
    
    @property
    def tf_one_hot_encoding_features(self) -> List[str]:
        return [f"tf_{col}" for col in self.one_hot_encoding_features]
    
    def unwanted_columns(self) -> List[str]:
        feature= [
            self.CustomerID,
            self.Count,
            self.Country,
            self.City,
            self.State,
            self.Zip_Code,
            self.Lat_Long,
            self.Latitude,
            self.Longitude,
            self.Churn_Value,
            self.Churn_Score,
            self.CLTV,
            self.Churn_Reason

        ]
        return feature
    
    def input_feature(self) -> List[str]:
        in_feature = self.one_hot_encoding_features + self.non_one_hont_encoder
        return in_feature
    
    def vector_assembler_output(self) -> str:
        return "va_input_features"
    
    def target_indexed_label(self) -> str:
        return f"indexed_{self.target_column}"
    


