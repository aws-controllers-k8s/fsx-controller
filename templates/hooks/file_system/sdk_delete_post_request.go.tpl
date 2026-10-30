	// Delete is async: FSx stays DELETING for 5-10 min, so keep the finalizer
	// until it is gone. Later reconciles short-circuit in
	// sdk_delete_pre_build_request until FileSystemNotFound.
	if err == nil {
		return r, requeueWaitWhileDeleting
	}
