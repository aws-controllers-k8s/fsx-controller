	// Delete is async: the association stays DELETING until FSx removes it, so
	// keep the finalizer. Record the lifecycle from the response (falling back
	// to DELETING) so Status shows the delete in progress. Later reconciles are
	// held by the deletable.when guard until Describe reports
	// DataRepositoryAssociationNotFound.
	if err == nil {
		setDeletingLifecycle(r, resp)
		return r, requeueWaitWhileDeleting
	}
