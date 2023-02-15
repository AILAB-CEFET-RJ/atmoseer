import numpy as np
from sklearn.metrics import confusion_matrix
import pandas as pd
import sklearn.metrics as skl 


NO_RAIN = 0
WEAK_RAIN = 1
MODERATE_RAIN = 2
STRONG_RAIN = 3
EXTREME_RAIN = 4

'''
  https://stackoverflow.com/questions/59935155/how-to-calculate-mean-bias-errormbe-in-python
'''
def mean_bias_error(y_true, y_pred):
  MBE = np.mean(y_pred - y_true)
  return MBE

def get_events_per_precipitation_level(y):
  no_rain = np.where(np.any(y<=0., axis=1))
  weak_rain = np.where(np.any((y>0.) & (y<=5.), axis=1))
  moderate_rain = np.where(np.any((y>5.) & (y<=25.), axis=1))
  strong_rain = np.where(np.any((y>25.) & (y<=50.), axis=1))
  extreme_rain = np.where(np.any(y>50., axis=1))
  return no_rain, weak_rain, moderate_rain, strong_rain, extreme_rain

def export_confusion_matrix_to_latex(y_true, y_pred):
  no_rain_true, weak_rain_true, moderate_rain_true, strong_rain_true, extreme_rain_true = get_events_per_precipitation_level(y_true)
  no_rain_pred, weak_rain_pred, moderate_rain_pred, strong_rain_pred, extreme_rain_pred = get_events_per_precipitation_level(y_pred)

  y_true_class = np.zeros_like(y_true)
  y_pred_class = np.zeros_like(y_pred)
  y_true_class[no_rain_true] = NO_RAIN
  y_pred_class[no_rain_pred] = NO_RAIN
  y_true_class[weak_rain_true] = WEAK_RAIN
  y_pred_class[weak_rain_pred] = WEAK_RAIN
  y_true_class[moderate_rain_true] = MODERATE_RAIN
  y_pred_class[moderate_rain_pred] = MODERATE_RAIN
  y_true_class[strong_rain_true] = STRONG_RAIN
  y_pred_class[strong_rain_pred] = STRONG_RAIN
  y_true_class[extreme_rain_true] = EXTREME_RAIN
  y_pred_class[extreme_rain_pred] = EXTREME_RAIN
  
  print('Resultado classification_report : ')
  print(skl.classification_report(y_true_class, y_pred_class))
  # target_names = ['No Rain', 'Weak Rain', 'Moderate Rain', 'Strong Rain']
  df = pd.DataFrame(
      confusion_matrix(y_true_class, y_pred_class, labels=[0,1,2,3,4]), 
      index=['true:No Rain', 'true:Weak Rain', 'true:Moderate Rain', 'true:Strong Rain', 'true:Extreme Rain', ], 
      columns=['pred:No Rain', 'pred:Weak Rain', 'pred:Moderate Rain', 'pred:Strong Rain', 'pred:Extreme Rain', ], 
  )
  print(df.to_latex())
  print()

'''
  MAE (mean absolute error) and MBE (mean bias error) values are computed for each precipitation level.
'''
def export_results_to_latex(y_true, y_pred):
  export_confusion_matrix_to_latex(y_true, y_pred)

  no_rain_true, weak_rain_true, moderate_rain_true, strong_rain_true, extreme_rain_true = get_events_per_precipitation_level(y_true)
  no_rain_pred, weak_rain_pred, moderate_rain_pred, strong_rain_pred, extreme_rain_pred = get_events_per_precipitation_level(y_pred)

  if no_rain_pred[0].size > 0:
    mse_no_rain = skl.mean_absolute_error(y_true[no_rain_true], y_pred[no_rain_true])
    mbe_no_rain = mean_bias_error(y_true[no_rain_true], y_pred[no_rain_true])
  else:
    mse_no_rain = mbe_no_rain = 'n/a'

  if weak_rain_pred[0].size > 0:
    mse_weak_rain = skl.mean_absolute_error(y_true[weak_rain_true], y_pred[weak_rain_true])
    mbe_weak_rain = mean_bias_error(y_true[weak_rain_true], y_pred[weak_rain_true])
  else:
    mse_weak_rain = mbe_weak_rain = 'n/a'

  if moderate_rain_pred[0].size > 0:
    mse_moderate_rain = skl.mean_absolute_error(y_true[moderate_rain_true], y_pred[moderate_rain_true])
    mbe_moderate_rain = mean_bias_error(y_true[moderate_rain_true], y_pred[moderate_rain_true])
  else:
    mse_moderate_rain = mbe_moderate_rain = 'n/a'

  if strong_rain_pred[0].size > 0:
    mse_strong_rain = skl.mean_absolute_error(y_true[strong_rain_true], y_pred[strong_rain_true])
    mbe_strong_rain = mean_bias_error(y_true[strong_rain_true], y_pred[strong_rain_true])
  else:
    mse_strong_rain = mbe_strong_rain = 'n/a'

  if extreme_rain_pred[0].size > 0:
    mse_extreme_rain = skl.mean_absolute_error(y_true[extreme_rain_true], y_pred[extreme_rain_true])
    mbe_extreme_rain = mean_bias_error(y_true[extreme_rain_true], y_pred[extreme_rain_true])
  else:
    mse_extreme_rain = mbe_extreme_rain = 'n/a'
  
  df = pd.DataFrame()
  df['level'] = ['No rain', 'Weak', 'Moderate', 'Strong', 'Extreme']
  df['qty_true'] = [no_rain_true[0].shape[0], weak_rain_true[0].shape[0], moderate_rain_true[0].shape[0], strong_rain_true[0].shape[0], extreme_rain_true[0].shape[0]]
  df['qty_pred'] = [no_rain_pred[0].shape[0], weak_rain_pred[0].shape[0], moderate_rain_pred[0].shape[0], strong_rain_pred[0].shape[0], extreme_rain_pred[0].shape[0]]
  df['mae'] = [mse_no_rain, mse_weak_rain, mse_moderate_rain, mse_strong_rain, mse_extreme_rain]
  df['mbe'] = [mbe_no_rain, mbe_weak_rain, mbe_moderate_rain, mbe_strong_rain, mbe_extreme_rain]
  print(df.to_latex())