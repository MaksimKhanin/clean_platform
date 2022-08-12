
from datetime import datetime, timezone, timedelta
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from . import ml_PSQL_queries as querylib
from . import ml_utils
import pandas as pd
from sklearn.utils import resample

NON_FEATURE_COLUMNS = {"date", "month", "year", "symbol",
                       "target", "marketCap_grow", "sector", "period",
                       "currency"}

def _transform_column_types(df):
    columns = df.columns
    stopColumns = {'symbol','sector','currency','period', 'month','year'}
    dateColumns = {"date"}
    for each_col in columns:
        if each_col in stopColumns:
            continue
        elif each_col in dateColumns:
            df[each_col] = df[each_col].astype("datetime64")
        else:
            df[each_col] = df[each_col].astype("float64").round(5)
    return df



def _create_mnth_year(df):
    df["month"] = df['date'].dt.month
    df["year"] = df['date'].dt.year
    return df

def _stmnt_prep_USD(days_before=1095):

    NOT_FEATURE_COLUNMS = ["symbol", "month", "year", "date"]
    COLUMNS_TO_FILL_0 = ['dividendYield', 'averageInventory', 'inventoryTurnover', 'interestCoverage']

    stmnt_since_date = datetime.utcnow().replace(tzinfo=timezone(timedelta(hours=0))) - timedelta(days=days_before)

    # quering data
    stmnts = ml_utils.get_data_from_db(querylib.QUERY_STATEMENTS,
                                       params=(stmnt_since_date.isoformat(),
                                               stmnt_since_date.isoformat(),
                                               'USD',))

    stmnts = _transform_column_types(stmnts)

    features = stmnts.groupby(["symbol", "month", "year", "date"]).mean().reset_index().sort_values("date").copy()
    features.loc[:, COLUMNS_TO_FILL_0] = features.loc[:, COLUMNS_TO_FILL_0].fillna(0)
    features = features[~features["close"].isnull()]
    features = features.fillna(-1)

    for each_column in features.columns:
        if each_column in NOT_FEATURE_COLUNMS:
            continue
        shift = features.groupby(["symbol"])[each_column].shift(1)
        features[each_column+"_ch"] = (features[each_column] - shift) / shift
        shift2 = features.groupby(["symbol"])[each_column].shift(2)
        features[each_column+"_ch2"] = (features[each_column] - shift2) / shift2

    features= features.fillna(-1)
    features = features.replace(np.inf, 999999999)
    features = features.replace(-np.inf, -999999999)

    return features

def _prep_class_target(features):
    features = _prep_reg_target(features)
    features["class_target"] = features["reg_target"] > 1
    return features.drop(columns="reg_target")

def _prep_reg_target(features):
    features["reg_target"] = (features.sort_values("date").groupby(["symbol"])['close'].shift(-1)/features['close'])
    features = features.dropna(subset=["reg_target"])
    return features


def _class_rebalansing(Xy):
    positive_class = Xy[Xy["class_target"] == True]
    negative_class = Xy[Xy["class_target"] == False]
    if len(positive_class) > len(negative_class):
        new_negative_class = resample(negative_class,
                                      replace=True,
                                      n_samples=len(positive_class))
        return pd.concat([positive_class, new_negative_class])
    else:
        new_positive_class = resample(positive_class,
                                      replace=True,
                                      n_samples=len(negative_class))

        return pd.concat([new_positive_class, negative_class])

def _stmnt_to_Xy(features, return_target=None, rebalance=True):

    NON_FEATURE_COLUMNS = ["symbol", "month", "year", "date"]

    # markTheTarget
    if return_target in ('class'):
        Xy = _prep_class_target(features).replace([np.inf, -np.inf], np.nan).dropna()
        if rebalance:
            Xy = _class_rebalansing(Xy)
        X = Xy.drop(columns=["class_target"]+NON_FEATURE_COLUMNS)
        y = Xy["class_target"]
        return X, y
    elif return_target in ('reg'):
        Xy = _prep_reg_target(features).replace([np.inf, -np.inf], np.nan).dropna()
        X = Xy.drop(columns=["reg_target"]+NON_FEATURE_COLUMNS)
        y = Xy["reg_target"]
        return X, y
    else:
        X = features.replace([np.inf, -np.inf], np.nan).dropna().drop(columns=NON_FEATURE_COLUMNS)
        return X

def return_stmnt_scores(model_id):
    df = _stmnt_prep_USD()
    X = _stmnt_to_Xy(df, return_target=False)
    model = ml_utils.load_model_from_db(model_id)
    df["statement_score"] = model.predict_proba(X)[:, 1].round(4)
    return df[["symbol", "date", "sector", "statement_score"]].drop_duplicates(subset=["symbol", "date"])

def upload_stmnt_scores_df(model_id):
    ml_utils.upload_df_db(return_stmnt_scores(model_id), "ml.stmnt_scores")

def update_statementAnalyzer():
    X, y = _stmnt_to_Xy(_stmnt_prep_USD(days_before=600), return_target='class', rebalance=True)
    StatementAnalyzer = RandomForestClassifier()
    StatementAnalyzer.fit(X, y)
    ml_utils.save_model_in_db("statement_analyzer", StatementAnalyzer)