from siametl.quality import DataQuality

class QualityHandler:
    def __init__(self):
        pass

    def validate_no_nulls(self, df, column_name):
        DataQuality.validate_no_nulls(df, column_name)

    def validate_threshold(self, df, column_name, threshold=0.1):
        DataQuality.validate_threshold(df, column_name, threshold)
