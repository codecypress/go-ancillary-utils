package cypressutils

const const_LOWER = "abcdefghijklmnopqrstuvwxyz"
const const_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
const const_DIGITS = "0123456789"
const const_PUNCTUATIONS = "!@#&+?"

type PasswordBuilder struct {
	useLower, useUpper, useDigits, usePunctuation bool
}

func (builder *PasswordBuilder) UseLower(useLower bool) *PasswordBuilder {
	builder.useLower = useLower
	return builder
}

func (builder *PasswordBuilder) UseUpper(useUpper bool) *PasswordBuilder {
	builder.useUpper = useUpper
	return builder
}

func (builder *PasswordBuilder) UseDigits(useDigits bool) *PasswordBuilder {
	builder.useDigits = useDigits
	return builder
}

func (builder *PasswordBuilder) UsePunctuation(usePunctuation bool) *PasswordBuilder {
	builder.usePunctuation = usePunctuation
	return builder
}

func (builder *PasswordBuilder) Build() *Passwords {
	return &Passwords{builder}
}

func NewPasswordBuilder() *PasswordBuilder {
	return &PasswordBuilder{
		useLower:       true,
		useUpper:       true,
		useDigits:      false,
		usePunctuation: false,
	}
}

type Passwords struct {
	builder *PasswordBuilder
}

func (password *Passwords) Generate(size int) string {
	if !password.builder.useLower && !password.builder.useUpper && !password.builder.useDigits && !password.builder.usePunctuation {
		return ""
	}

	allowableChars := ""

	if password.builder.useLower {
		allowableChars += const_LOWER
	}
	if password.builder.useUpper {
		allowableChars += const_UPPER
	}
	if password.builder.useDigits {
		allowableChars += const_DIGITS
	}
	if password.builder.usePunctuation {
		allowableChars += const_PUNCTUATIONS
	}

	numberOfCodePoints := len(allowableChars)

	randomChars := make([]uint8, size)
	for i := 0; i < size; i++ {
		randomChars[i] = allowableChars[RandomNum(numberOfCodePoints)]
	}
	return string(randomChars)
}
