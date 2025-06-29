from sklearn.ensemble import GradientBoostingClassifier
import numpy as np 
import logging

NAIVE_SUBSAMPLING_KEEP_RATIO = 0.05

# With negative subsampling (train/val/test): 13934/3142/10219.

def apply_subsampling(X, y, subsampling_strategy):
    assert subsampling_strategy in  ("NAIVE", "NEGATIVE")
    if subsampling_strategy == "NAIVE":
        return apply_naive_subsampling(X, y)
    else:
        return apply_negative_subsampling(X, y)

def apply_naive_subsampling(X, y):
    """
    Naive subsampling: keep all the positive instances and subsample the negative instances completely at random.
    """
    y_eq_zero_idxs = np.where(y == 0)[0]
    y_gt_zero_idxs = np.where(y > 0)[0]
    logging.info(f' - Original sizes (target=0)/(target>0): ({y_eq_zero_idxs.shape[0]})/({y_gt_zero_idxs.shape[0]})')

    logging.info(f" - Using keep ratio = {NAIVE_SUBSAMPLING_KEEP_RATIO}.")

    # Setting numpy seed value
    np.random.seed(0)

    mask = np.random.choice([True, False], size=y.shape[0], p=[
                            NAIVE_SUBSAMPLING_KEEP_RATIO, 1.0-NAIVE_SUBSAMPLING_KEEP_RATIO])
    y_train_subsample_idxs = np.where(mask == True)[0]

    logging.info(f" - Subsample (total) size: {y_train_subsample_idxs.shape[0]}")
    idxs = np.intersect1d(y_eq_zero_idxs, y_train_subsample_idxs)
    logging.info(f" - Subsample (target=0) size: {idxs.shape[0]}")

    idxs = np.union1d(idxs, y_gt_zero_idxs)
    X, y = X[idxs], y[idxs]
    y_eq_zero_idxs = np.where(y == 0)[0]
    y_gt_zero_idxs = np.where(y > 0)[0]
    logging.info(f' - Resulting sizes (target=0)/(target>0): ({y_eq_zero_idxs.shape[0]})/({y_gt_zero_idxs.shape[0]})')

    return X, y

def apply_negative_subsampling(X_train, y_train):
    original_shape_X_train = X_train.shape
    
    X_train = X_train.reshape(len(X_train), -1)
    y_train_binarized = np.copy(y_train)
    y_train_binarized[y_train_binarized>0] = 1    
    
    positive_indices = np.where(y_train_binarized == 1)[0]

    print("num examples in X_train:", len(X_train))
    print("num pos in X_train:", len(positive_indices))

    ###
    # Apply the steps of the negative sampling procedure
    ###

    # Step 1: Train "pilot" model
    clf = train_pilot_model(X_train, y_train_binarized)

    # Step 2: Score the negative examples with the pilot model
    y_proba_normalized = score_negative_examples(clf, X_train, y_train_binarized)

    # Step 3: Sample the negative examples proportionally to their scores
    negative_indices = sample_from_negative_examples(X_train, y_train_binarized, y_proba_normalized)
    
    X_train_sampled = np.concatenate((X_train[positive_indices], X_train[negative_indices]))
    y_train_sampled = np.concatenate((y_train[positive_indices], y_train[negative_indices]))
    
    X_train_sampled = X_train_sampled.reshape((len(X_train_sampled), original_shape_X_train[1], original_shape_X_train[2]))
    y_train_sampled = y_train_sampled.reshape(-1, 1)

    return X_train_sampled, y_train_sampled

def train_pilot_model(X_train, y_train):
    '''
    Train the pilot model on a balanced dataset. This balanced dataset is built in 
    such a way that it has equal amounts of positive and negative examples. If there 
    are P positive examples in the original training set, then P negative
    examples will be uniformly sampled from it to put into the balanced dataset. All 
    the positive examples in the original dataset are put in the balanced dataset.
    '''
    y_eq_zero_idxs = np.where(y_train == 0)[0]
    y_gt_zero_idxs = np.where(y_train > 0)[0]
    logging.info(f"Amounts of neg/pos examples: {len(y_eq_zero_idxs)}/{len(y_gt_zero_idxs)}")

    X_train_positives = X_train[y_gt_zero_idxs]
    X_train_negatives = X_train[y_eq_zero_idxs]

    num_positive_examples = len(X_train_positives)
    num_negative_examples = len(X_train_negatives)

    assert num_positive_examples < num_negative_examples

    num_negative_examples_to_sample = min(num_positive_examples, num_negative_examples)

    positive_indices = np.random.choice(num_positive_examples, size=num_negative_examples_to_sample, replace=False)
    negative_indices = np.random.choice(num_negative_examples, size=num_negative_examples_to_sample, replace=False)

    X_train_balanced = np.concatenate((X_train_positives[positive_indices], X_train_negatives[negative_indices]))
    y_train_balanced = np.concatenate((np.ones(num_negative_examples_to_sample), np.zeros(num_negative_examples_to_sample)))

    assert len(y_train_balanced) == 2*num_negative_examples_to_sample

    # Create a GradientBoostingClassifier object with default hyperparameters
    clf = GradientBoostingClassifier()

    # Train the classifier on the balanced training dataset
    clf.fit(X_train_balanced, y_train_balanced)
    
    return clf

def score_negative_examples(clf, X_train, y_train):
    y_eq_zero_idxs = np.where(y_train == 0)[0]
    X_train_negatives = X_train[y_eq_zero_idxs]
    y_train_negatives = y_train[y_eq_zero_idxs]

    # Get predicted probabilities on the negative samples
    y_proba = clf.predict_proba(X_train_negatives)

    # The predicted probabilities for the negative class (class 0) are in the first column
    y_proba_negative = y_proba[:, 0]

    # Normalize the probabilities to sum to 1
    y_proba_normalized = y_proba_negative / np.sum(y_proba_negative)

    N = 5
    logging.info(f"Normalized scores for the first {N} negative examples: {y_proba_normalized[:N]}")
    logging.info(f"Correct labels for the first {N} negative examples: {y_train_negatives[:N]}")
    
    return y_proba_normalized

def sample_from_negative_examples(X_train, y_train, y_proba_normalized):
    y_eq_zero_idxs = np.where(y_train == 0)[0]
    y_gt_zero_idxs = np.where(y_train > 0)[0]
    
    positive_examples = X_train[y_gt_zero_idxs]
    num_positive_examples = len(positive_examples)

    # Sample the indices using the normalized probabilities
    negative_sampled_idxs = np.random.choice(y_eq_zero_idxs, size=num_positive_examples, replace=False, p=y_proba_normalized)

    return negative_sampled_idxs
