use tantivy::schema::{
    FAST, Field, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions,
};

pub const SCORE_FIELD: &str = "score";

#[derive(Clone, Copy)]
pub struct Fields {
    pub did: Field,
    pub handle: Field,
    pub display_name: Field,
    pub avatar_cid: Field,
    pub score: Field,
    pub terms: Field,
    pub labels: Field,
    pub created_at: Field,
    pub verification: Field,
}

pub fn build() -> (Schema, Fields) {
    let mut builder = Schema::builder();
    let terms_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_fieldnorms(false)
            .set_index_option(IndexRecordOption::Basic),
    );

    // The order in which you add fields to the builder is important
    // so make sure to always add new fields to the end.
    let did = builder.add_text_field("did", STRING | STORED);
    let handle = builder.add_text_field("handle", STORED);
    let display_name = builder.add_text_field("display_name", STORED);
    let avatar_cid = builder.add_text_field("avatar_cid", STORED);
    let score = builder.add_u64_field(SCORE_FIELD, FAST);
    let terms = builder.add_text_field("terms", terms_options);
    let labels = builder.add_text_field("labels", STORED);
    let created_at = builder.add_text_field("created_at", STORED);
    let verification = builder.add_text_field("verification", STORED);

    let fields = Fields {
        did,
        handle,
        display_name,
        avatar_cid,
        score,
        terms,
        labels,
        created_at,
        verification,
    };
    (builder.build(), fields)
}
