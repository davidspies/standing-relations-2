use std::sync::Arc;

pub(crate) struct RelationData {
    name: Option<String>,
    type_name: &'static str,
    hidden: bool,
    children: Vec<Arc<RelationData>>,
}
impl RelationData {
    pub(crate) fn new(type_name: &'static str, children: Vec<Arc<RelationData>>) -> Self {
        Self {
            name: None,
            type_name,
            hidden: false,
            children,
        }
    }
}
