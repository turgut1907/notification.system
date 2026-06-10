-- Seed a sample template so the README curl examples work out of the box.
INSERT INTO templates (id, name, channel, content, required_variables)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    'verification_code',
    'sms',
    'Your verification code is {{code}}. Expires in {{expire_minutes}} minutes.',
    '["code","expire_minutes"]'::jsonb
)
ON CONFLICT (id) DO NOTHING;
