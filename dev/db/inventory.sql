CREATE TABLE public.hosts (
    id INT NOT NULL,
    account character varying(10),
    tags jsonb,
    PRIMARY KEY (id)
);

INSERT INTO public.hosts VALUES (1, '12234', '{"Sat": {"env": [null, "prod"]},
                                               "client": {"ansible_group": ["foo", "bar"]}}');
INSERT INTO public.hosts VALUES (2, '34634', '{"Sat": {"env": ["prod"]},
                                               "client": {"ansible_group": ["foo", "bar"]}}');
INSERT INTO public.hosts VALUES (3, '34523', '{"Sat": {"env": ["prod"]},
                                               "client": {"empty_arr": []}}');
INSERT INTO public.hosts VALUES (4, '23423', '{"Sat": {"env": ["prod"]},
                                               "client": {"ansible_group": ["foo", "bar"]}}');
