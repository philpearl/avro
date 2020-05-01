package avro

func skip(r *Buffer, l int64) error {
	_, err := r.Next(int(l))
	return err
}
