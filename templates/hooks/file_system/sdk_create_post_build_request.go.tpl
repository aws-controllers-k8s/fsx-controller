	// The generated builder omits a password whose referenced Secret resolves to
	// an empty value, so creation could succeed without it.
	if err := rejectEmptyResolvedPasswords(desired, input); err != nil {
		return nil, err
	}
