	// In hooks.go because it needs imports this template cannot add to sdk.go.
	if err = setDeleteVolumeConfiguration(r, input); err != nil {
		return nil, err
	}
