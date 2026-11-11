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
    pub labels: Field,
    pub score: Field,
    pub terms: Field,
}

pub fn build() -> (Schema, Fields) {
    let mut builder = Schema::builder();
    let terms_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_fieldnorms(false)
            .set_index_option(IndexRecordOption::Basic),
    );
    let fields = Fields {
        did: builder.add_text_field("did", STRING | STORED),
        handle: builder.add_text_field("handle", STORED),
        display_name: builder.add_text_field("display_name", STORED),
        avatar_cid: builder.add_text_field("avatar_cid", STORED),
        labels: builder.add_text_field("labels", STORED),
        score: builder.add_u64_field(SCORE_FIELD, FAST),
        terms: builder.add_text_field("terms", terms_options),
    };
    (builder.build(), fields)
}
